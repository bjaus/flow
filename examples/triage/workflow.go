// Package triage demonstrates a typed plan-and-review workflow.
package triage

import (
	"github.com/bjaus/flow"
)

type Ticket struct {
	Title string `json:"title"`
}
type Plan struct {
	Steps    []string `json:"steps"`
	Approved bool     `json:"approved"`
}

// Workflow returns a reusable typed workflow definition.
func Workflow() flow.Workflow[Ticket, Plan] {
	plan := flow.Agent[Ticket, Plan]("planner", func(ticket Ticket) string { return "Draft a plan for: " + ticket.Title })
	review := flow.Human("review", func(plan Plan, decision flow.Decision) Plan { plan.Approved = decision.Approved; return plan }, func(Plan) string { return "Approve this plan?" })
	return flow.Define("triage", "Plan and review a ticket", flow.Then(plan, review))
}
