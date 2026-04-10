package controller

import (
	"context"
	"encoding/json"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fusekiv1alpha1 "github.com/larsw/k8s-fuseki-operator/api/v1alpha1"
)

type renderedSecurityPolicyBundle struct {
	Policies []renderedSecurityPolicy `json:"policies"`
}

type renderedSecurityPolicy struct {
	Name         string                            `json:"name"`
	Description  string                            `json:"description,omitempty"`
	Rules        []renderedSecurityPolicyRule      `json:"rules"`
	GraphTagging []renderedGraphSecurityTaggingRule `json:"graphTagging,omitempty"`
}

type renderedSecurityPolicyRule struct {
	Target         renderedDatasetAccessTarget `json:"target"`
	Actions        []string                    `json:"actions"`
	Effect         string                      `json:"effect"`
	ExpressionType string                      `json:"expressionType"`
	Expression     string                      `json:"expression"`
	Subjects       []renderedSecuritySubject   `json:"subjects"`
}

type renderedGraphSecurityTaggingRule struct {
	DatasetRef     string                    `json:"datasetRef"`
	ExpressionType string                    `json:"expressionType"`
	TagPredicate   string                    `json:"tagPredicate,omitempty"`
	Actions        []string                  `json:"actions"`
	Subjects       []renderedSecuritySubject `json:"subjects"`
}

type renderedDatasetAccessTarget struct {
	DatasetRef string `json:"datasetRef"`
	NamedGraph string `json:"namedGraph,omitempty"`
}

type renderedSecuritySubject struct {
	Type  string `json:"type"`
	Value string `json:"value,omitempty"`
	Claim string `json:"claim,omitempty"`
}

func resolveDatasetSecurityPolicies(ctx context.Context, c client.Client, dataset *fusekiv1alpha1.Dataset) ([]fusekiv1alpha1.SecurityPolicy, []string, error) {
	if dataset == nil || len(dataset.Spec.SecurityPolicies) == 0 {
		return nil, nil, nil
	}

	policies := make([]fusekiv1alpha1.SecurityPolicy, 0, len(dataset.Spec.SecurityPolicies))
	missing := make([]string, 0)
	for _, ref := range dataset.Spec.SecurityPolicies {
		if ref.Name == "" {
			continue
		}

		var policy fusekiv1alpha1.SecurityPolicy
		err := c.Get(ctx, client.ObjectKey{Namespace: dataset.Namespace, Name: ref.Name}, &policy)
		if err == nil {
			policies = append(policies, policy)
			continue
		}
		if apierrors.IsNotFound(err) {
			missing = append(missing, ref.Name)
			continue
		}
		return nil, nil, err
	}

	sort.Slice(policies, func(i, j int) bool {
		return policies[i].Name < policies[j].Name
	})
	sort.Strings(missing)
	return policies, missing, nil
}

func renderDatasetSecurityPolicyBundle(policies []fusekiv1alpha1.SecurityPolicy) (string, error) {
	bundle := renderedSecurityPolicyBundle{Policies: make([]renderedSecurityPolicy, 0, len(policies))}
	for _, policy := range policies {
		rendered := renderedSecurityPolicy{
			Name:        policy.Name,
			Description: policy.Spec.Description,
			Rules:       make([]renderedSecurityPolicyRule, 0, len(policy.Spec.Rules)),
		}
		for _, rule := range policy.Spec.Rules {
			actions := make([]string, 0, len(rule.Actions))
			for _, action := range rule.Actions {
				actions = append(actions, string(action))
			}
			subjects := make([]renderedSecuritySubject, 0, len(rule.Subjects))
			for _, subject := range rule.Subjects {
				subjects = append(subjects, renderedSecuritySubject{
					Type:  string(subject.Type),
					Value: subject.Value,
					Claim: subject.Claim,
				})
			}
			rendered.Rules = append(rendered.Rules, renderedSecurityPolicyRule{
				Target: renderedDatasetAccessTarget{
					DatasetRef: rule.Target.DatasetRef.Name,
					NamedGraph: rule.Target.NamedGraph,
				},
				Actions:        actions,
				Effect:         string(rule.DesiredEffect()),
				ExpressionType: string(rule.DesiredExpressionType()),
				Expression:     rule.Expression,
				Subjects:       subjects,
			})
		}
		for _, tag := range policy.Spec.GraphTagging {
			actions := make([]string, 0, len(tag.Actions))
			for _, action := range tag.Actions {
				actions = append(actions, string(action))
			}
			subjects := make([]renderedSecuritySubject, 0, len(tag.Subjects))
			for _, subject := range tag.Subjects {
				subjects = append(subjects, renderedSecuritySubject{
					Type:  string(subject.Type),
					Value: subject.Value,
					Claim: subject.Claim,
				})
			}
			rendered.GraphTagging = append(rendered.GraphTagging, renderedGraphSecurityTaggingRule{
				DatasetRef:     tag.DatasetRef.Name,
				ExpressionType: string(tag.ExpressionType),
				TagPredicate:   tag.TagPredicate,
				Actions:        actions,
				Subjects:       subjects,
			})
		}
		bundle.Policies = append(bundle.Policies, rendered)
	}

	payload, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return "", err
	}
	return string(payload) + "\n", nil
}

func datasetReferencesSecurityPolicy(dataset *fusekiv1alpha1.Dataset, policyName string) bool {
	if dataset == nil || policyName == "" {
		return false
	}

	for _, ref := range dataset.Spec.SecurityPolicies {
		if ref.Name == policyName {
			return true
		}
	}
	return false
}

func securityPolicyTargetsDataset(policy *fusekiv1alpha1.SecurityPolicy, datasetName string) bool {
	if policy == nil || datasetName == "" {
		return false
	}

	for _, rule := range policy.Spec.Rules {
		if rule.Target.DatasetRef.Name == datasetName {
			return true
		}
	}
	for _, tag := range policy.Spec.GraphTagging {
		if tag.DatasetRef.Name == datasetName {
			return true
		}
	}
	return false
}

func securityPolicyTargetDatasetNames(policy *fusekiv1alpha1.SecurityPolicy) []string {
	if policy == nil {
		return nil
	}

	seen := make(map[string]struct{}, len(policy.Spec.Rules)+len(policy.Spec.GraphTagging))
	names := make([]string, 0, len(policy.Spec.Rules)+len(policy.Spec.GraphTagging))
	for _, rule := range policy.Spec.Rules {
		name := rule.Target.DatasetRef.Name
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	for _, tag := range policy.Spec.GraphTagging {
		name := tag.DatasetRef.Name
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
