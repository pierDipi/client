/*
Copyright 2022 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package eventtype

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/printers"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingv1beta1 "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"knative.dev/eventing/pkg/utils"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/kmeta"

	"knative.dev/client/pkg/dynamic"
	"knative.dev/client/pkg/kn/commands"
	"knative.dev/client/pkg/kn/commands/flags"
)

var queryExample = `
  # Describe event types matching the given CE SQL expression
  kn eventtype query --cesql <expression> 

  # Generate triggers for matching event types in YAML format
  kn eventtype query --cesql <expression> --show triggers -o yaml

  # Subscribe to matching event types (skipping event types without broker reference)
  kn eventtype query --cesql <expression> --sink <sink> --continuous

  # Subscribe to matching event types and show triggers
  kn eventtype query --cesql <expression> --sink <sink> --show triggers --continuous
`

// NewEventTypeQueryCommand represents command to query eventtypes.
func NewEventTypeQueryCommand(p *commands.KnParams) *cobra.Command {
	sinkFlags := &flags.SinkFlags{IncludeOffline: true}
	machineReadablePrintFlags := genericclioptions.NewPrintFlags("")

	continuous := false
	show := ""

	cmd := &cobra.Command{
		Use:     "query",
		Short:   "Query or subscribe to event types",
		Example: queryExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			namespace, err := p.GetNamespace(cmd)
			if err != nil {
				return err
			}

			client, err := p.NewDynamicClient(namespace)
			if err != nil {
				return err
			}

			queryGVR := schema.GroupVersionResource{
				Group:    "eventing.knative.dev",
				Version:  "v1beta1",
				Resource: "eventtypequeries",
			}

			/*
				apiVersion: eventing.knative.dev/v1beta1
				kind: EventTypeQuery
				metadata:
				  generateName: query-
				spec:
				  filters:
					- cesql: "type == 'dev.knative.sources.ping'"
			*/
			ceSQLExpression := cmd.Flag("cesql").Value.String()
			query := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "eventing.knative.dev/v1beta1",
					"kind":       "EventTypeQuery",
					"metadata": map[string]interface{}{
						"generateName": "query-",
						"namespace":    namespace,
					},
					"spec": map[string]interface{}{
						"continuous": continuous,
						"filters": []map[string]interface{}{
							{"cesql": ceSQLExpression},
						},
					},
				},
			}

			if continuous {
				destination, err := sinkFlags.ResolveSink(cmd.Context(), client, namespace)
				if err != nil || destination == nil {
					return fmt.Errorf("failed to resolve sink: %w", err)
				}
				spec := query.Object["spec"].(map[string]interface{})
				spec["subscriber"] = destination
			}

			q, err := client.RawClient().
				Resource(queryGVR).
				Namespace(namespace).
				Create(cmd.Context(), query, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("failed to query event types: %w", err)
			}

			eventTypeRefs, err := waitQueryResponse(cmd, err, client, queryGVR, q.GetNamespace(), q.GetName())
			if err != nil {
				return err
			}

			if show == "triggers" {
				brokers, err := collectBrokers(cmd, eventTypeRefs, p)
				if err != nil {
					return err
				}

				triggers, err := generateTriggers(cmd, client, sinkFlags, ceSQLExpression, namespace, brokers)
				if err != nil {
					return err
				}
				if err := printTriggers(cmd, machineReadablePrintFlags, triggers); err != nil {
					return err
				}

				return nil
			}

			eventTypes, err := collectEventTypes(cmd, eventTypeRefs, p)
			if err != nil {
				return err
			}

			if machineReadablePrintFlags.OutputFlagSpecified() {
				printer, err := machineReadablePrintFlags.ToPrinter()
				if err != nil {
					return err
				}
				return printer.PrintObj(eventTypes, cmd.OutOrStdout())
			}

			for _, et := range eventTypes.Items {
				if err := describeEventtype(cmd.OutOrStdout(), &et, false); err != nil {
					return fmt.Errorf("failed to describe event tyep %s/%s: %w", et.GetNamespace(), et.GetName(), err)
				}
			}

			return nil
		},
	}

	cmd.Flags().String("cesql", "", `--cesql "type == 'dev.knative.sources.ping'"`)
	cmd.Flags().BoolP("verbose", "v", false, "-v, --verbose")
	cmd.Flags().BoolVar(&continuous, "continuous", false, "--continuous")
	cmd.Flags().StringVar(&show, "show", "", "--show triggers")
	sinkFlags.Add(cmd)
	machineReadablePrintFlags.AddFlags(cmd)
	cmd.Flag("output").Usage = fmt.Sprintf("Output format. One of: %s.", strings.Join(machineReadablePrintFlags.AllowedFormats(), "|"))

	_ = cmd.MarkFlagRequired("cesql")

	commands.AddNamespaceFlags(cmd.Flags(), true)

	return cmd
}

func generateTriggers(cmd *cobra.Command, client dynamic.KnDynamicClient, sinkFlags *flags.SinkFlags, ceSQLExpression, namespace string, brokers []BrokerRef) (*eventingv1.TriggerList, error) {
	destination, err := sinkFlags.ResolveSink(cmd.Context(), client, namespace)
	if err != nil || destination == nil {
		return nil, fmt.Errorf("failed to resolve sink: %w", err)
	}

	triggers := eventingv1.TriggerList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "TriggerList",
			APIVersion: eventingv1.SchemeGroupVersion.String(),
		},
	}
	for _, br := range brokers {
		tr := eventingv1.Trigger{
			ObjectMeta: metav1.ObjectMeta{
				Name:      utils.ToDNS1123Subdomain(kmeta.ChildName(br.Name, ceSQLExpression)),
				Namespace: namespace,
			},
			Spec: eventingv1.TriggerSpec{
				Broker:     br.Name,
				Filters:    []eventingv1.SubscriptionsAPIFilter{{CESQL: ceSQLExpression}},
				Subscriber: *destination,
			},
		}

		triggers.Items = append(triggers.Items, tr)
	}
	return &triggers, nil
}

type BrokerRef struct {
	Name      string
	EventType *eventingv1beta1.EventType
}

func collectBrokers(cmd *cobra.Command, eventTypesRefs []duckv1.KReference, p *commands.KnParams) ([]BrokerRef, error) {
	var brokers []BrokerRef
	brokerSet := sets.NewString()

	eventTypes, err := collectEventTypes(cmd, eventTypesRefs, p)
	if err != nil {
		return nil, err
	}

	for _, et := range eventTypes.Items {
		if len(et.Spec.Broker) == 0 {
			continue
		}

		if brokerSet.Has(et.Spec.Broker) {
			continue
		}
		brokerSet.Insert(et.Spec.Broker)

		brokers = append(brokers, BrokerRef{
			Name:      et.Spec.Broker,
			EventType: &et,
		})
	}
	maybePrintBrokers(cmd, brokers)
	return brokers, nil
}

func collectEventTypes(cmd *cobra.Command, eventTypesRefs []duckv1.KReference, p *commands.KnParams) (*eventingv1beta1.EventTypeList, error) {
	eventTypes := make([]eventingv1beta1.EventType, 0, len(eventTypesRefs))
	for _, etRef := range eventTypesRefs {
		c, err := p.NewEventingV1beta1Client(etRef.Namespace)
		if err != nil {
			return nil, fmt.Errorf("failed to create eventing client: %w", err)
		}

		et, err := c.GetEventtype(cmd.Context(), etRef.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get event type %s/%s: %w", et.GetNamespace(), et.GetName(), err)
		}

		eventTypes = append(eventTypes, *et)
	}
	return &eventingv1beta1.EventTypeList{
		TypeMeta: metav1.TypeMeta{Kind: "EventTypeList", APIVersion: "eventing.knative.dev/v1beta1"},
		Items:    eventTypes,
	}, nil
}

func maybePrintResponse(cmd *cobra.Command, eventTypes []duckv1.KReference) {
	if v, err := cmd.Flags().GetBool("verbose"); err != nil && v {
		jTypes, _ := json.MarshalIndent(eventTypes, "", "  ")
		_, _ = fmt.Fprintf(cmd.OutOrStderr(), string(jTypes))
	}
}

func maybePrintBrokers(cmd *cobra.Command, brokers []BrokerRef) {
	if v, err := cmd.Flags().GetBool("verbose"); err != nil && v {
		jTypes, _ := json.MarshalIndent(brokers, "", "  ")
		_, _ = fmt.Fprintf(cmd.OutOrStderr(), string(jTypes))
	}
}

func printTriggers(cmd *cobra.Command, machineReadablePrintFlags *genericclioptions.PrintFlags, triggers *eventingv1.TriggerList) error {
	if machineReadablePrintFlags.OutputFlagSpecified() {
		printer, err := machineReadablePrintFlags.ToPrinter()
		if err != nil {
			return err
		}
		return printer.PrintObj(triggers, cmd.OutOrStdout())
	}

	printer := printers.JSONPrinter{}
	return printer.PrintObj(triggers, cmd.OutOrStdout())
}

func waitQueryResponse(cmd *cobra.Command, err error, client dynamic.KnDynamicClient, queryGVR schema.GroupVersionResource, namespace, name string) ([]duckv1.KReference, error) {
	var eventTypes []duckv1.KReference
	err = wait.PollImmediate(330*time.Millisecond, time.Minute, func() (bool, error) {
		q, err := client.RawClient().
			Resource(queryGVR).
			Namespace(namespace).
			Get(cmd.Context(), name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("failed to get query result: %w", err)
		}
		statusUnstr, ok := q.UnstructuredContent()["status"]
		if !ok {
			return false, nil
		}
		status := statusUnstr.(map[string]interface{})
		types, ok := status["eventTypes"]
		if !ok {
			return false, nil
		}
		tps := types.([]interface{})
		jTps, err := json.Marshal(tps)
		if err != nil {
			return false, fmt.Errorf("failed to marshal event types: %w", err)
		}
		if err := json.Unmarshal(jTps, &eventTypes); err != nil {
			return false, fmt.Errorf("failed to unmarshal event types: %w", err)
		}
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to wait for a query response: %w", err)
	}
	maybePrintResponse(cmd, eventTypes)
	return eventTypes, nil
}
