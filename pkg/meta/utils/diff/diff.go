package diff

import (
	"fmt"
	"io"
	"text/tabwriter"

	"inspr.dev/inspr/pkg/meta"
	metautils "inspr.dev/inspr/pkg/meta/utils"
	"inspr.dev/inspr/pkg/utils"
)

// Kind represents a kind of difference between two structures
type Kind int

// Kinds of diff
const (
	AppKind Kind = 1 << iota
	NodeKind
	MetaKind
	ChannelKind
	TypeKind
	BoundaryKind
	FieldKind
	AnnotationKind
	AliasKind
	EnvironmentKind
)

// Operation represents an operation that has been applied in a diff
type Operation int

// The kinds of operation
const (
	Delete Operation = 1 << iota
	Update
	Create
)

// Difference is the most basic diff structure, it represents a difference between two apps.
// The object carries information abaout what field differs from one app to another,
// the value of that field on the original app and the value of that field on the current app.
type Difference struct {
	Field     string `json:"field"`
	From      string `json:"from"`
	To        string `json:"to"`
	Kind      Kind
	Name      string
	Operation Operation
}

// Change encapsulates all differences between two apps and carries the
// information about the scope those apps exist in the app tree.
type Change struct {
	Scope     string       `json:"scope"`
	Diff      []Difference `json:"diff"`
	Kind      Kind
	Operation Operation
	changelog *Changelog
}

//Changelog log of all changes between two app trees.
type Changelog []Change

//Diff returns the changelog between two app trees.
func Diff(appOrig *meta.App, appCurr *meta.App) (Changelog, error) {
	var err error
	cl := Changelog{}
	cl, err = cl.diff(appOrig, appCurr, "")
	return cl, err
}

//Print is an auxiliar method used for displaying a Changelog
func (cl Changelog) Print(out io.Writer) {
	var w *tabwriter.Writer

	for _, change := range cl {
		fmt.Fprintln(out, "On:", change.Scope)
		w = tabwriter.NewWriter(out, 12, 0, 3, ' ', tabwriter.Debug)
		fmt.Fprintln(w, "Field\t From\t To")
		for _, diff := range change.Diff {
			fmt.Fprintf(
				w,
				"%s\t %s\t %s\n",
				diff.Field,
				diff.From,
				diff.To,
			)
		}
		w.Flush()
	}
}

func (cl *Changelog) diff(from, to *meta.App, scope string) (Changelog, error) {

	change := Change{
		Scope:     scope,
		changelog: cl,
	}

	err := change.diffMetadata(from.Meta.Name, AppKind, from.Meta, to.Meta, "")
	if err != nil {
		return Changelog{}, err
	}

	err = change.diffAppSpec(from.Spec, to.Spec)
	if err != nil {
		return Changelog{}, err
	}

	if len(change.Diff) > 0 {
		*cl = append(*cl, change)
	}
	return *cl, nil
}

func (change *Change) diffAppSpec(from, to meta.AppSpec) error {
	err := change.diffNodes(from.Node, to.Node)
	if err != nil {
		return err
	}

	change.diffApps(from.Apps, to.Apps)

	err = change.diffChannels(from.Channels, to.Channels)
	if err != nil {
		return err
	}

	err = change.diffTypes(from.Types, to.Types)
	if err != nil {
		return err
	}

	change.diffBoudaries(from.Boundary, to.Boundary)
	change.diffAliases(from.Aliases, to.Aliases)
	return nil
}

func (change *Change) diffAliases(from, to metautils.MAliases) {
	fromSet, _ := metautils.MakeStrSet(from)
	toSet, _ := metautils.MakeStrSet(to)

	set := metautils.DisjunctSet(fromSet, toSet)

	for alias := range set {
		var op Operation
		_, orig := from[alias]

		fromStr := "<nil>"
		toStr := "<nil>"

		if orig {
			fromStr = "{...}"
			op = Delete
		} else {
			toStr = "{...}"
			op = Create
		}

		change.Diff = append(change.Diff, Difference{
			Field:     fmt.Sprintf("Spec.Aliases[%s]", alias),
			From:      fromStr,
			To:        toStr,
			Kind:      AliasKind,
			Operation: op,
			Name:      alias,
		})
		change.Kind |= AliasKind
		change.Operation |= op
	}

	intersection := metautils.IntersectSet(fromSet, toSet)

	for alias := range intersection {
		fromApp := from[alias]
		toApp := to[alias]
		if fromApp.Resource != toApp.Resource {
			change.Diff = append(change.Diff, Difference{
				Field:     fmt.Sprintf("Spec.Aliases[%s]", alias),
				From:      fromApp.Resource,
				To:        toApp.Resource,
				Kind:      AliasKind,
				Name:      alias,
				Operation: Update,
			})
			change.Kind |= AliasKind
			change.Operation |= Update
		}
	}
}

func (change *Change) diffNodes(from, to meta.Node) error {
	err := change.diffMetadata(from.Meta.Name, NodeKind, from.Meta, to.Meta, "Spec.Node.")
	if err != nil {
		return err
	}

	if from.Spec.Image != to.Spec.Image {
		change.Diff = append(change.Diff, Difference{
			Field:     "Spec.Node.Spec.Image",
			From:      from.Spec.Image,
			To:        to.Spec.Image,
			Kind:      NodeKind,
			Operation: Update,
		})
		change.Kind |= NodeKind
		change.Operation |= Update
	}

	if from.Spec.Replicas != to.Spec.Replicas {
		change.Diff = append(change.Diff, Difference{
			Field:     "Spec.Node.Spec.Replicas",
			From:      fmt.Sprint(from.Spec.Replicas),
			To:        fmt.Sprint(to.Spec.Replicas),
			Kind:      NodeKind,
			Operation: Update,
		})
		change.Kind |= NodeKind
		change.Operation |= Update
	}
	change.diffEnv(from.Spec.Environment, to.Spec.Environment)

	return nil
}

func (change *Change) diffEnv(from utils.EnvironmentMap, to utils.EnvironmentMap) {
	for key, fromValue := range from {
		if toValue, ok := to[key]; ok {
			if toValue != fromValue {
				change.Diff = append(change.Diff, Difference{
					Field:     fmt.Sprintf("Spec.Node.Spec.Environment[%s]", key),
					From:      fromValue,
					To:        toValue,
					Kind:      EnvironmentKind,
					Name:      key,
					Operation: Update,
				})
				change.Kind |= EnvironmentKind
				change.Operation |= Update
			}
		} else {
			change.Diff = append(change.Diff, Difference{
				Field:     fmt.Sprintf("Spec.Node.Spec.Environment[%s]", key),
				From:      fromValue,
				To:        "<nil>",
				Kind:      EnvironmentKind,
				Name:      key,
				Operation: Delete,
			})
			change.Operation |= Delete
			change.Kind |= EnvironmentKind
		}
	}

	for key, toValue := range to {
		if _, ok := from[key]; !ok {
			change.Diff = append(change.Diff, Difference{
				Field:     fmt.Sprintf("Spec.Node.Spec.Environment[%s]", key),
				From:      "<nil>",
				To:        toValue,
				Kind:      EnvironmentKind,
				Name:      key,
				Operation: Create,
			})

			change.Operation |= Create
			change.Kind |= EnvironmentKind
		}
	}

}
func (change *Change) diffBoudaries(boundOrig, boundCurr meta.AppBoundary) {
	var orig string
	var curr string

	origSet, _ := metautils.MakeStrSet(boundOrig.Channels.Input)
	currSet, _ := metautils.MakeStrSet(boundCurr.Channels.Input)

	inputSet := metautils.DisjunctSet(origSet, currSet)
	inputOrig, _ := metautils.MakeStrSet(boundOrig.Channels.Input)
	for k := range inputSet {
		var op Operation
		orig = "<nil>"
		curr = "<nil>"

		if inputOrig[k] {
			orig = k
			op = Delete
		} else {
			curr = k
			op = Create
		}

		change.Diff = append(change.Diff, Difference{
			Field:     "Spec.Boundary.Input",
			From:      orig,
			To:        curr,
			Kind:      BoundaryKind,
			Name:      k,
			Operation: op,
		})

		change.Kind |= BoundaryKind
		change.Operation |= op
	}

	origSetOut, _ := metautils.MakeStrSet(boundOrig.Channels.Output)
	currSetOut, _ := metautils.MakeStrSet(boundCurr.Channels.Output)

	outputSet := metautils.DisjunctSet(origSetOut, currSetOut)
	outputOrig, _ := metautils.MakeStrSet(boundOrig.Channels.Output)
	for k := range outputSet {
		var op Operation
		orig = "<nil>"
		curr = "<nil>"

		if outputOrig[k] {
			orig = k
			op = Delete
		} else {
			curr = k
			op = Create
		}

		change.Diff = append(change.Diff, Difference{
			Field:     "Spec.Boundary.Output",
			From:      orig,
			To:        curr,
			Kind:      BoundaryKind,
			Operation: op,
			Name:      k,
		})
		change.Kind |= BoundaryKind
		change.Operation |= op
	}
}

func (change *Change) diffApps(from, to metautils.MApps) {
	fromSet, _ := metautils.MakeStrSet(from)
	toSet, _ := metautils.MakeStrSet(to)

	set := metautils.DisjunctSet(fromSet, toSet)

	for k := range set {
		var op Operation
		_, orig := from[k]

		fromStr := "<nil>"
		toStr := "<nil>"

		if orig {
			fromStr = "{...}"
			op = Delete
		} else {
			toStr = "{...}"
			op = Create
			newScope, _ := metautils.JoinScopes(change.Scope, k)
			*change.changelog, _ = change.changelog.diff(&meta.App{}, to[k], newScope)
		}

		change.Diff = append(change.Diff, Difference{
			Field:     fmt.Sprintf("Spec.Apps[%s]", k),
			From:      fromStr,
			To:        toStr,
			Kind:      AppKind,
			Operation: op,
			Name:      k,
		})
		change.Kind |= AppKind
		change.Operation |= op
	}

	intersection := metautils.IntersectSet(fromSet, toSet)

	for app := range intersection {
		fromApp := from[app]
		toApp := to[app]

		newScope, _ := metautils.JoinScopes(change.Scope, fromApp.Meta.Name)
		change.changelog.diff(fromApp, toApp, newScope)
	}

}

func (change *Change) diffChannels(from, to metautils.MChannels) error {
	fromSet, _ := metautils.MakeStrSet(from)
	toSet, _ := metautils.MakeStrSet(to)

	disjunction := metautils.DisjunctSet(fromSet, toSet)

	for ch := range disjunction {
		_, orig := from[ch]
		from := "<nil>"
		to := "<nil>"
		var op Operation
		if orig {
			from = "{...}"
			op = Delete
		} else {
			to = "{...}"
			op = Create
		}

		change.Diff = append(change.Diff, Difference{
			Field:     fmt.Sprintf("Spec.Channels[%s]", ch),
			From:      from,
			To:        to,
			Kind:      ChannelKind,
			Operation: op,
			Name:      ch,
		})
		change.Kind |= ChannelKind
		change.Operation |= op
	}

	intersection := metautils.IntersectSet(fromSet, toSet)

	for ch := range intersection {
		fromCh := from[ch]
		toCh := to[ch]
		if fromCh.Spec.Type != toCh.Spec.Type {
			change.Diff = append(change.Diff, Difference{
				Field:     fmt.Sprintf("Spec.Channels[%s].Spec.Type", ch),
				From:      fromCh.Spec.Type,
				To:        toCh.Spec.Type,
				Kind:      ChannelKind,
				Operation: Update,
				Name:      ch,
			})
			change.Kind |= ChannelKind
			change.Operation |= Update
		}

		err := change.diffMetadata(ch, ChannelKind, fromCh.Meta, toCh.Meta, "Spec.Channels["+ch+"].")
		if err != nil {
			return err
		}
	}

	return nil
}

func (change *Change) diffTypes(from, to metautils.MTypes) error {
	fromSet, _ := metautils.MakeStrSet(from)
	toSet, _ := metautils.MakeStrSet(to)

	disjunction := metautils.DisjunctSet(fromSet, toSet)

	for ct := range disjunction {
		_, orig := from[ct]

		fromStr := "<nil>"
		toStr := "<nil>"
		var op Operation
		if orig {
			fromStr = "{...}"
			op = Delete

		} else {
			toStr = "{...}"
			op = Create
		}

		change.Diff = append(change.Diff, Difference{
			Field:     fmt.Sprintf("Spec.Types[%s]", ct),
			From:      fromStr,
			To:        toStr,
			Kind:      TypeKind,
			Operation: op,
			Name:      ct,
		})
		change.Kind |= TypeKind
		change.Operation |= op
	}

	intersection := metautils.IntersectSet(fromSet, toSet)

	for ct := range intersection {
		fromCT := from[ct]
		toCT := to[ct]

		if string(fromCT.Schema) != string(toCT.Schema) {
			change.Diff = append(change.Diff, Difference{
				Field:     fmt.Sprintf("Spec.Types[%s].Spec.Schema", ct),
				From:      string(fromCT.Schema),
				To:        string(toCT.Schema),
				Kind:      TypeKind,
				Operation: Update,
				Name:      ct,
			})
			change.Kind |= TypeKind
			change.Operation |= Update
		}

		err := change.diffMetadata(ct, TypeKind, fromCT.Meta, toCT.Meta, fmt.Sprintf("Spec.Types[%s].", ct))
		if err != nil {
			return err
		}

	}

	return nil
}

func (change *Change) diffMetadata(parentElement string, parentKind Kind, from, to meta.Metadata, ctx string) error {

	if from.Name != to.Name {
		change.Diff = append(change.Diff, Difference{
			Field:     ctx + "Meta.Name",
			From:      from.Name,
			To:        to.Name,
			Kind:      MetaKind | parentKind,
			Operation: Update,
			Name:      parentElement,
		})
		change.Kind |= MetaKind | parentKind
		change.Operation |= Update
	}

	if from.Reference != to.Reference {
		change.Diff = append(change.Diff, Difference{
			Field:     ctx + "Meta.Reference",
			From:      from.Reference,
			To:        to.Reference,
			Kind:      MetaKind | parentKind,
			Operation: Update,
			Name:      parentElement,
		})
		change.Kind |= MetaKind | parentKind
		change.Operation |= Update
	}

	if from.Parent != to.Parent {
		change.Diff = append(change.Diff, Difference{
			Field:     ctx + "Meta.Parent",
			From:      from.Parent,
			To:        to.Parent,
			Kind:      MetaKind | parentKind,
			Operation: Update,
			Name:      parentElement,
		})
		change.Kind |= MetaKind | parentKind
		change.Operation |= Update
	}

	fromSet, _ := metautils.MakeStrSet(from.Annotations)
	toSet, _ := metautils.MakeStrSet(to.Annotations)

	fromSet.AppendSet(toSet)

	parentAnnUpdate := false
	annFrom := ""
	annTo := ""
	annDiff := []Difference{}
	for k := range fromSet {
		var op Operation
		annotationChange := false
		fromVal, fromOk := from.Annotations[k]
		toVal, toOk := to.Annotations[k]

		if fromOk != toOk {
			parentAnnUpdate = true
			annotationChange = true
			if fromVal == "" {
				fromVal = "<nil>"
				op = Create
			} else {
				toVal = "<nil>"
				op = Delete
			}
			annFrom += fmt.Sprintf("%s:%s;", k, fromVal)
			annTo += fmt.Sprintf("%s:%s;", k, toVal)
		} else if fromVal != toVal {
			parentAnnUpdate = true
			annotationChange = true
			op = Update
			annFrom += fmt.Sprintf("%s:%s;", k, fromVal)
			annTo += fmt.Sprintf("%s:%s;", k, toVal)
		}

		if annotationChange {
			annDiff = append(annDiff, Difference{
				Field:     fmt.Sprintf("%sMeta.Annotations[%s]", ctx, k),
				From:      fromVal,
				To:        toVal,
				Kind:      MetaKind | AnnotationKind,
				Name:      k,
				Operation: op,
			})
		}
	}
	if parentAnnUpdate {
		change.Diff = append(change.Diff, annDiff...)
		change.Kind |= MetaKind | AnnotationKind
		change.Operation |= Update
	}

	return nil
}
