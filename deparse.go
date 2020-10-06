package pg_query

import (
	"fmt"
	"strings"

	"github.com/kr/pretty"
	nodes "github.com/tomaszjonak/pg_query_go/nodes"
)

type DeparseContext struct {
	Context string
}

func Deparse(tree ParsetreeList) (string, error) {
	results := make([]string, len(tree.Statements))
	for i, item := range tree.Statements {
		result, err := DeparseItem(item)
		if err != nil {
			return "", err
		}
		results[i] = result
	}
	return strings.Join(results, "; "), nil
}

func DeparseItem(item nodes.Node) (string, error) {
	ctx := DeparseContext{}
	result, err := ctx.deparseItem(item)
	if err != nil {
		return "", err
	}
	return result, nil
}

func (c DeparseContext) deparseItem(node nodes.Node) (string, error) {
	switch node.(type) {
	case nodes.A_ArrayExpr:
		return c.deparseA_ArrayExpr(node.(nodes.A_ArrayExpr))
	case *nodes.A_ArrayExpr:
		return c.deparseA_ArrayExpr(*node.(*nodes.A_ArrayExpr))
	case nodes.A_Const:
		return c.deparseA_Const(node.(nodes.A_Const))
	case *nodes.A_Const:
		return c.deparseA_Const(*node.(*nodes.A_Const))
	case nodes.A_Expr:
		switch node.(nodes.A_Expr).Kind {
		case nodes.AEXPR_OP:
			return c.deparseA_Expr(node.(nodes.A_Expr))
		case nodes.AEXPR_OP_ANY:
			return c.deparseA_ExprAny(node.(nodes.A_Expr))
		//case nodes.AEXPR_OP_ALL:
		//case nodes.AEXPR_DISTINCT:
		//case nodes.AEXPR_NOT_DISTINCT:
		case nodes.AEXPR_NULLIF:
			return c.deparseA_ExprNullif(node.(nodes.A_Expr))
		//case nodes.AEXPR_OF:
		case nodes.AEXPR_IN:
			return c.deparseA_ExprIn(node.(nodes.A_Expr))
		case nodes.AEXPR_LIKE:
			return c.deparseA_ExprLike(node.(nodes.A_Expr))
		//case nodes.AEXPR_ILIKE:
		//case nodes.AEXPR_SIMILAR:
		case nodes.AEXPR_BETWEEN,
			nodes.AEXPR_NOT_BETWEEN,
			nodes.AEXPR_BETWEEN_SYM,
			nodes.AEXPR_NOT_BETWEEN_SYM:
			return c.deparseA_ExprBetween(node.(nodes.A_Expr))
		//case nodes.AEXPR_PAREN:
		default:
			return "", fmt.Errorf("Can't deparse: %# v", pretty.Formatter(node))
		}
	case *nodes.A_Expr:
		switch node.(*nodes.A_Expr).Kind {
		case nodes.AEXPR_OP:
			return c.deparseA_Expr(*node.(*nodes.A_Expr))
		case nodes.AEXPR_OP_ANY:
			return c.deparseA_ExprAny(*node.(*nodes.A_Expr))
		//case nodes.AEXPR_OP_ALL:
		//case nodes.AEXPR_DISTINCT:
		//case nodes.AEXPR_NOT_DISTINCT:
		case nodes.AEXPR_NULLIF:
			return c.deparseA_ExprNullif(*node.(*nodes.A_Expr))
		//case nodes.AEXPR_OF:
		case nodes.AEXPR_IN:
			return c.deparseA_ExprIn(*node.(*nodes.A_Expr))
		case nodes.AEXPR_LIKE:
			return c.deparseA_ExprLike(*node.(*nodes.A_Expr))
		//case nodes.AEXPR_ILIKE:
		//case nodes.AEXPR_SIMILAR:
		case nodes.AEXPR_BETWEEN,
			nodes.AEXPR_NOT_BETWEEN,
			nodes.AEXPR_BETWEEN_SYM,
			nodes.AEXPR_NOT_BETWEEN_SYM:
			return c.deparseA_ExprBetween(*node.(*nodes.A_Expr))
		//case nodes.AEXPR_PAREN:
		default:
			return "", fmt.Errorf("Can't deparse: %# v", pretty.Formatter(node))
		}
	case nodes.A_Indices:
		return c.deparseA_Indices(node.(nodes.A_Indices))
	case *nodes.A_Indices:
		return c.deparseA_Indices(*node.(*nodes.A_Indices))
	case nodes.A_Indirection:
		return c.deparseA_Indirection(node.(nodes.A_Indirection))
	case *nodes.A_Indirection:
		return c.deparseA_Indirection(*node.(*nodes.A_Indirection))
	case nodes.A_Star:
		return c.deparseA_Star(node.(nodes.A_Star))
	case *nodes.A_Star:
		return c.deparseA_Star(*node.(*nodes.A_Star))
	case nodes.Alias:
		return c.deparseAlias(node.(nodes.Alias))
	case *nodes.Alias:
		return c.deparseAlias(*(node.(*nodes.Alias)))
	case nodes.BoolExpr:
		switch node.(nodes.BoolExpr).Boolop {
		case nodes.AND_EXPR:
			return c.deparseBoolExprAnd(node.(nodes.BoolExpr))
		case nodes.OR_EXPR:
			return c.deparseBoolExprOr(node.(nodes.BoolExpr))
		case 0x2: //NOT_EXPR
			return c.deparseBoolExprNot(node.(nodes.BoolExpr))
		default:
			return "", fmt.Errorf("Can't deparse: %# v", pretty.Formatter(node))
		}
	case *nodes.BoolExpr:
		switch node.(*nodes.BoolExpr).Boolop {
		case nodes.AND_EXPR:
			return c.deparseBoolExprAnd(*node.(*nodes.BoolExpr))
		case nodes.OR_EXPR:
			return c.deparseBoolExprOr(*node.(*nodes.BoolExpr))
		case 0x2: //NOT_EXPR
			return c.deparseBoolExprNot(*node.(*nodes.BoolExpr))
		default:
			return "", fmt.Errorf("Can't deparse: %# v", pretty.Formatter(node))
		}
	case nodes.BooleanTest:
		return c.deparseBooleanTest(node.(nodes.BooleanTest))
	case *nodes.BooleanTest:
		return c.deparseBooleanTest(*node.(*nodes.BooleanTest))
	case nodes.CaseExpr:
		return c.deparseCaseExpr(node.(nodes.CaseExpr))
	case *nodes.CaseExpr:
		return c.deparseCaseExpr(*node.(*nodes.CaseExpr))
	case nodes.CaseWhen:
		return c.deparseCaseWhen(node.(nodes.CaseWhen))
	case *nodes.CaseWhen:
		return c.deparseCaseWhen(*node.(*nodes.CaseWhen))
	case nodes.CoalesceExpr:
		return c.deparseCoalesceExpr(node.(nodes.CoalesceExpr))
	case *nodes.CoalesceExpr:
		return c.deparseCoalesceExpr(*node.(*nodes.CoalesceExpr))
	case nodes.ColumnDef:
		return c.deparseColumnDef(node.(nodes.ColumnDef))
	case *nodes.ColumnDef:
		return c.deparseColumnDef(*node.(*nodes.ColumnDef))
	case nodes.ColumnRef:
		return c.deparseColumnRef(node.(nodes.ColumnRef))
	case *nodes.ColumnRef:
		return c.deparseColumnRef(*node.(*nodes.ColumnRef))
	case nodes.CommonTableExpr:
		return c.deparseCommonTableExpr(node.(nodes.CommonTableExpr))
	case *nodes.CommonTableExpr:
		return c.deparseCommonTableExpr(*node.(*nodes.CommonTableExpr))
	case nodes.Float:
		return node.(nodes.Float).Str, nil
	case *nodes.Float:
		return node.(*nodes.Float).Str, nil
	case nodes.FuncCall:
		return c.deparseFuncCall(node.(nodes.FuncCall))
	case *nodes.FuncCall:
		return c.deparseFuncCall(*node.(*nodes.FuncCall))
	case nodes.Integer:
		return fmt.Sprintf("%d", node.(nodes.Integer).Ival), nil
	case *nodes.Integer:
		return fmt.Sprintf("%d", node.(*nodes.Integer).Ival), nil
	case nodes.JoinExpr:
		return c.deparseJoinExpr(node.(nodes.JoinExpr))
	case *nodes.JoinExpr:
		return c.deparseJoinExpr(*node.(*nodes.JoinExpr))
	case nodes.Null:
		return "NULL", nil
	case nodes.NullTest:
		return c.deparseNullTest(node.(nodes.NullTest))
	case *nodes.NullTest:
		return c.deparseNullTest(*node.(*nodes.NullTest))
	case nodes.ParamRef:
		return c.deparseParamRef(node.(nodes.ParamRef))
	case *nodes.ParamRef:
		return c.deparseParamRef(*node.(*nodes.ParamRef))
	case nodes.RangeFunction:
		return c.deparseRangeFunction(node.(nodes.RangeFunction))
	case *nodes.RangeFunction:
		return c.deparseRangeFunction(*node.(*nodes.RangeFunction))
	case nodes.RangeSubselect:
		return c.deparseRangeSubselect(node.(nodes.RangeSubselect))
	case *nodes.RangeSubselect:
		return c.deparseRangeSubselect(*node.(*nodes.RangeSubselect))
	case nodes.RangeVar:
		return c.deparseRangeVar(node.(nodes.RangeVar))
	case *nodes.RangeVar:
		return c.deparseRangeVar(*node.(*nodes.RangeVar))
	case nodes.RawStmt:
		return c.deparseRawStmt(node.(nodes.RawStmt))
	case *nodes.RawStmt:
		return c.deparseRawStmt(*node.(*nodes.RawStmt))
	case nodes.ResTarget:
		return c.deparseResTarget(node.(nodes.ResTarget))
	case *nodes.ResTarget:
		return c.deparseResTarget(*node.(*nodes.ResTarget))
	case nodes.RowExpr:
		return c.deparseRowExpr(node.(nodes.RowExpr))
	case *nodes.RowExpr:
		return c.deparseRowExpr(*node.(*nodes.RowExpr))
	case nodes.SelectStmt:
		return c.deparseSelect(node.(nodes.SelectStmt))
	case *nodes.SelectStmt:
		return c.deparseSelect(*node.(*nodes.SelectStmt))
	case nodes.SortBy:
		return c.deparseSortBy(node.(nodes.SortBy))
	case *nodes.SortBy:
		return c.deparseSortBy(*node.(*nodes.SortBy))
	case nodes.String:
		switch c.Context {
		case "select":
			return fmt.Sprintf(`"%s"`, node.(nodes.String).Str), nil
		case "a_const":
			return fmt.Sprintf(`'%s'`, strings.Replace(node.(nodes.String).Str, `'`, `''`, -1)), nil
		case "func_call", "type_name", "operator", "defname_as":
			return node.(nodes.String).Str, nil
		default:
			return fmt.Sprintf(`"%s"`, strings.Replace(node.(nodes.String).Str, `"`, `""`, -1)), nil
		}
	case *nodes.String:
		switch c.Context {
		case "select":
			return fmt.Sprintf(`"%s"`, node.(*nodes.String).Str), nil
		case "a_const":
			return fmt.Sprintf(`'%s'`, strings.Replace(node.(*nodes.String).Str, `'`, `''`, -1)), nil
		case "func_call", "type_name", "operator", "defname_as":
			return node.(*nodes.String).Str, nil
		default:
			return fmt.Sprintf(`"%s"`, strings.Replace(node.(*nodes.String).Str, `"`, `""`, -1)), nil
		}
	case nodes.SubLink:
		return c.deparseSubLink(node.(nodes.SubLink))
	case *nodes.SubLink:
		return c.deparseSubLink(*node.(*nodes.SubLink))
	case nodes.TypeCast:
		return c.deparseTypeCast(node.(nodes.TypeCast))
	case *nodes.TypeCast:
		return c.deparseTypeCast(*node.(*nodes.TypeCast))
	case nodes.TypeName:
		return c.deparseTypeName(node.(nodes.TypeName))
	case *nodes.TypeName:
		return c.deparseTypeName(*node.(*nodes.TypeName))
	case nodes.WithClause:
		return c.deparseWithClause(node.(nodes.WithClause))
	case *nodes.WithClause:
		return c.deparseWithClause(*(node.(*nodes.WithClause)))
	case nodes.WindowDef:
		return c.deparseWindowDef(node.(nodes.WindowDef))
	case *nodes.WindowDef:
		return c.deparseWindowDef(*node.(*nodes.WindowDef))
	default:
		return "", fmt.Errorf("Can't deparse: %# v", pretty.Formatter(node))
	}
}

func (c DeparseContext) deparseItemList(list nodes.List) ([]string, error) {
	results := make([]string, len(list.Items))
	for i, item := range list.Items {
		result, err := c.deparseItem(item)
		if err != nil {
			return []string{}, err
		}
		results[i] = result
	}
	return results, nil
}

func (c DeparseContext) deparseA_ArrayExpr(node nodes.A_ArrayExpr) (string, error) {
	elementItems, err := c.deparseItemList(node.Elements)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ARRAY[%s]", strings.Join(elementItems, ", ")), nil
}

func (c DeparseContext) deparseA_Const(node nodes.A_Const) (string, error) {
	ctx := DeparseContext{Context: "a_const"}
	return ctx.deparseItem(node.Val)
}

func (c DeparseContext) deparseA_Expr(node nodes.A_Expr) (string, error) {
	output := []string{}
	var ctx DeparseContext
	if c.Context != "" {
		ctx = c
	} else {
		ctx = DeparseContext{Context: "a_expr"}
	}

	lexpr, err := ctx.deparseItem(node.Lexpr)
	if err != nil {
		return "", err
	}
	output = append(output, lexpr)
	rexpr, err := ctx.deparseItem(node.Rexpr)
	if err != nil {
		return "", err
	}
	output = append(output, rexpr)
	opctx := DeparseContext{Context: "operator"}
	operator, err := opctx.deparseItem(node.Name.Items[0])
	if err != nil {
		return "", err
	}
	result := strings.Join(output, fmt.Sprintf(" %s ", operator))
	if c.Context == "a_expr" {
		result = fmt.Sprintf("(%s)", result)
	}
	return result, nil
}

func (c DeparseContext) deparseA_ExprAny(node nodes.A_Expr) (string, error) {
	output := []string{}
	lexpr, err := c.deparseItem(node.Lexpr)
	if err != nil {
		return "", err
	}
	output = append(output, lexpr)
	ctx := DeparseContext{Context: "operator"}
	operator, err := ctx.deparseItem(node.Name.Items[0])
	if err != nil {
		return "", err
	}
	rexpr, err := c.deparseItem(node.Rexpr)
	if err != nil {
		return "", err
	}
	output = append(output, fmt.Sprintf("ANY(%s)", rexpr))
	return strings.Join(output, fmt.Sprintf(" %s ", operator)), nil
}

func (c DeparseContext) deparseA_ExprBetween(node nodes.A_Expr) (string, error) {
	var between string
	switch node.Kind {
	case nodes.AEXPR_BETWEEN:
		between = "BETWEEN"
	case nodes.AEXPR_NOT_BETWEEN:
		between = "NOT BETWEEN"
	case nodes.AEXPR_BETWEEN_SYM:
		between = "BETWEEN SYMMETRIC"
	case nodes.AEXPR_NOT_BETWEEN_SYM:
		between = "NOT BETWEEN SYMMETRIC"
	}
	lexpr, err := c.deparseItem(node.Lexpr)
	if err != nil {
		return "", err
	}

	var rexpr string
	switch node.Rexpr.(type) {
	case nodes.List:
		rexprItems, err := c.deparseItemList(node.Rexpr.(nodes.List))
		if err != nil {
			return "", err
		}
		rexpr = strings.Join(rexprItems, " AND ")
	case nodes.Node:
		rexprItem, err := c.deparseItem(node.Rexpr)
		if err != nil {
			return "", err
		}
		rexpr = rexprItem
	}
	return fmt.Sprintf("%s %s %s", lexpr, between, rexpr), nil
}

func (c DeparseContext) deparseA_ExprIn(node nodes.A_Expr) (string, error) {
	lexpr, err := c.deparseItem(node.Lexpr)
	if err != nil {
		return "", err
	}
	ctx := DeparseContext{Context: "operator"}
	nameItems, err := ctx.deparseItemList(node.Name)
	if err != nil {
		return "", err
	}
	var operator string
	if nameItems[0] == "=" {
		operator = "IN"
	} else {
		operator = "NOT IN"
	}
	var rexpr string
	switch node.Rexpr.(type) {
	case nodes.List:
		rexprItems, err := c.deparseItemList(node.Rexpr.(nodes.List))
		if err != nil {
			return "", err
		}
		rexpr = strings.Join(rexprItems, ", ")
	case nodes.Node:
		rexprItem, err := c.deparseItem(node.Rexpr)
		if err != nil {
			return "", err
		}
		rexpr = rexprItem
	}
	return fmt.Sprintf("%s %s (%s)", lexpr, operator, rexpr), nil
}

func (c DeparseContext) deparseA_ExprLike(node nodes.A_Expr) (string, error) {
	rexpr, err := c.deparseItem(node.Rexpr)
	if err != nil {
		return "", err
	}
	ctx := DeparseContext{Context: "operator"}
	nameItems, err := ctx.deparseItemList(node.Name)
	if err != nil {
		return "", err
	}
	var operator string
	if nameItems[0] == "~~" {
		operator = "LIKE"
	} else {
		operator = "NOT LIKE"
	}
	lexpr, err := c.deparseItem(node.Lexpr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s %s %s", lexpr, operator, rexpr), nil
}

func (c DeparseContext) deparseA_ExprNullif(node nodes.A_Expr) (string, error) {
	lexpr, err := c.deparseItem(node.Lexpr)
	if err != nil {
		return "", err
	}
	rexpr, err := c.deparseItem(node.Rexpr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("NULLIF(%s, %s)", lexpr, rexpr), nil
}

func (c DeparseContext) deparseA_Indices(node nodes.A_Indices) (string, error) {
	uidx, err := c.deparseItem(node.Uidx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("[%s]", uidx), nil
}

func (c DeparseContext) deparseA_Indirection(node nodes.A_Indirection) (string, error) {
	output := []string{}
	arg, err := c.deparseItem(node.Arg)
	if err != nil {
		return "", err
	}
	output = append(output, arg)
	indirectionItems, err := c.deparseItemList(node.Indirection)
	if err != nil {
		return "", err
	}
	output = append(output, indirectionItems...)
	return strings.Join(output, ""), nil
}

func (c DeparseContext) deparseA_Star(node nodes.A_Star) (string, error) {
	return "*", nil
}

func (c DeparseContext) deparseAlias(node nodes.Alias) (string, error) {
	name := *node.Aliasname
	if node.Colnames.Items != nil {
		colnames_items, err := c.deparseItemList(node.Colnames)
		if err != nil {
			return "", err
		}
		name = fmt.Sprintf("%s(%s)", name, strings.Join(colnames_items, ", "))
	}
	return name, nil
}

func (c DeparseContext) deparseBooleanTest(node nodes.BooleanTest) (string, error) {
	arg, err := c.deparseItem(node.Arg)
	if err != nil {
		return "", err
	}
	var booltest string
	switch node.Booltesttype {
	case nodes.IS_TRUE:
		booltest = "IS TRUE"
	case nodes.IS_NOT_TRUE:
		booltest = "IS NOT TRUE"
	case 0x2:
		booltest = "IS FALSE"
	case 0x3:
		booltest = "IS NOT FALSE"
	case 0x4:
		booltest = "IS UNKNOWN"
	case 0x5:
		booltest = "IS NOT UNKNOWN"
	}
	return fmt.Sprintf("%s %s", arg, booltest), nil
}

func (c DeparseContext) deparseBoolExprAnd(node nodes.BoolExpr) (string, error) {
	output := []string{}
	for _, item := range node.Args.Items {
		result, err := c.deparseItem(item)
		if err != nil {
			return "", err
		}
		switch item.(type) {
		case nodes.BoolExpr:
			switch item.(nodes.BoolExpr).Boolop {
			case nodes.OR_EXPR:
				result = fmt.Sprintf("(%s)", result)
			}
		}
		output = append(output, result)
	}
	return strings.Join(output, " AND "), nil
}

func (c DeparseContext) deparseBoolExprNot(node nodes.BoolExpr) (string, error) {
	arg, err := c.deparseItem(node.Args.Items[0])
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("NOT %s", arg), nil
}

func (c DeparseContext) deparseBoolExprOr(node nodes.BoolExpr) (string, error) {
	output := []string{}
	for _, item := range node.Args.Items {
		result, err := c.deparseItem(item)
		if err != nil {
			return "", err
		}
		//TODO parentheses
		switch item.(type) {
		case nodes.BoolExpr:
			switch item.(nodes.BoolExpr).Boolop {
			case nodes.AND_EXPR, nodes.OR_EXPR:
				result = fmt.Sprintf("(%s)", result)
			}
		}
		output = append(output, result)
	}
	return strings.Join(output, " OR "), nil
}

func (c DeparseContext) deparseCaseExpr(node nodes.CaseExpr) (string, error) {
	output := []string{}
	output = append(output, "CASE")
	if node.Arg != nil {
		arg, err := c.deparseItem(node.Arg)
		if err != nil {
			return "", err
		}
		output = append(output, arg)
	}
	argItems, err := c.deparseItemList(node.Args)
	if err != nil {
		return "", err
	}
	output = append(output, argItems...)
	if node.Defresult != nil {
		output = append(output, "ELSE")
		defresult, err := c.deparseItem(node.Defresult)
		if err != nil {
			return "", err
		}
		output = append(output, defresult)
	}
	output = append(output, "END")
	return strings.Join(output, " "), nil
}

func (c DeparseContext) deparseCaseWhen(node nodes.CaseWhen) (string, error) {
	output := []string{}
	output = append(output, "WHEN")
	expr, err := c.deparseItem(node.Expr)
	if err != nil {
		return "", err
	}
	output = append(output, expr)
	output = append(output, "THEN")
	result, err := c.deparseItem(node.Result)
	if err != nil {
		return "", err
	}
	output = append(output, result)
	return strings.Join(output, " "), nil
}

func (c DeparseContext) deparseCoalesceExpr(node nodes.CoalesceExpr) (string, error) {
	argItems, err := c.deparseItemList(node.Args)
	if err != nil {
		return "", err
	}
	args := strings.Join(argItems, ", ")
	return fmt.Sprintf("COALESCE(%s)", args), nil
}

func (c DeparseContext) deparseColumnDef(node nodes.ColumnDef) (string, error) {
	output := []string{}
	output = append(output, *node.Colname)
	typeName, err := c.deparseItem(node.TypeName)
	if err != nil {
		return "", err
	}
	output = append(output, typeName)
	if node.RawDefault != nil {
		output = append(output, "USING")
		rawDefault, err := c.deparseItem(node.RawDefault)
		if err != nil {
			return "", err
		}
		output = append(output, rawDefault)
	}
	if node.Constraints.Items != nil {
		constraintItems, err := c.deparseItemList(node.Constraints)
		if err != nil {
			return "", err
		}
		output = append(output, constraintItems...)
	}
	return strings.Join(output, " "), nil
}

func (c DeparseContext) deparseColumnRef(node nodes.ColumnRef) (string, error) {
	output := []string{}

	fieldItems, err := c.deparseItemList(node.Fields)
	if err != nil {
		return "", err
	}
	output = append(output, fieldItems...)

	return strings.Join(output, "."), nil
}

func (c DeparseContext) deparseCommonTableExpr(node nodes.CommonTableExpr) (string, error) {
	output := []string{}
	output = append(output, *node.Ctename)
	if node.Aliascolnames.Items != nil {
		aliascolnameItems, err := c.deparseItemList(node.Aliascolnames)
		if err != nil {
			return "", err
		}
		output = append(output, fmt.Sprintf("(%s)", strings.Join(aliascolnameItems, ", ")))
	}
	ctequery, err := c.deparseItem(node.Ctequery)
	if err != nil {
		return "", err
	}
	output = append(output, fmt.Sprintf("AS (%s)", ctequery))
	return strings.Join(output, " "), nil
}

func (c DeparseContext) deparseFuncCall(node nodes.FuncCall) (string, error) {
	output := []string{}

	argItems, err := c.deparseItemList(node.Args)
	if err != nil {
		return "", err
	}
	if node.AggStar {
		argItems = append(argItems, "*")
	}
	args := strings.Join(argItems, ", ")

	ctx := DeparseContext{Context: "func_call"}
	funcnameItemsPre, err := ctx.deparseItemList(node.Funcname)
	if err != nil {
		return "", err
	}
	funcnameItems := funcnameItemsPre[:0]
	for _, fn := range funcnameItemsPre {
		if fn != "pg_catalog" {
			funcnameItems = append(funcnameItems, fn)
		}
	}
	funcname := strings.Join(funcnameItems, ".")

	var distinct string
	if node.AggDistinct {
		distinct = "DISTINCT "
	}

	output = append(output, fmt.Sprintf("%s(%s%s)", funcname, distinct, args))
	if node.Over != nil {
		over, err := c.deparseItem(node.Over)
		if err != nil {
			return "", err
		}
		output = append(output, fmt.Sprintf("OVER (%s)", over))
	}

	return strings.Join(output, " "), nil
}

func (c DeparseContext) deparseJoinExpr(node nodes.JoinExpr) (string, error) {
	output := []string{}
	larg, err := c.deparseItem(node.Larg)
	if err != nil {
		return "", err
	}
	output = append(output, larg)
	switch node.Jointype {
	case nodes.JOIN_INNER:
		if node.IsNatural {
			output = append(output, "NATURAL")
		} else if node.Quals == nil && node.UsingClause.Items == nil {
			output = append(output, "CROSS")
		}
	case nodes.JOIN_LEFT:
		output = append(output, "LEFT")
	case nodes.JOIN_FULL:
		output = append(output, "FULL")
	case nodes.JOIN_RIGHT:
		output = append(output, "RIGHT")
	}
	output = append(output, "JOIN")
	rarg, err := c.deparseItem(node.Rarg)
	if err != nil {
		return "", err
	}
	output = append(output, rarg)

	if node.Quals != nil {
		output = append(output, "ON")
		quals, err := c.deparseItem(node.Quals)
		if err != nil {
			return "", nil
		}
		output = append(output, quals)
	}

	if node.UsingClause.Items != nil {
		usingClauseItems, err := c.deparseItemList(node.UsingClause)
		if err != nil {
			return "", err
		}
		usingClause := strings.Join(usingClauseItems, ", ")
		output = append(output, fmt.Sprintf("USING (%s)", usingClause))
	}

	return strings.Join(output, " "), nil
}

func (c DeparseContext) deparseNullTest(node nodes.NullTest) (string, error) {
	output := []string{}
	arg, err := c.deparseItem(node.Arg)
	if err != nil {
		return "", err
	}
	output = append(output, arg)
	switch node.Nulltesttype {
	case nodes.IS_NULL:
		output = append(output, "IS NULL")
	case nodes.IS_NOT_NULL:
		output = append(output, "IS NOT NULL")
	}
	return strings.Join(output, " "), nil
}

func (c DeparseContext) deparseParamRef(node nodes.ParamRef) (string, error) {
	// count starts at 1 so this should be fine
	if node.Number == 0 {
		return "?", nil
	}
	return fmt.Sprintf("%d", node.Number), nil
}

func (c DeparseContext) deparseRangeFunction(node nodes.RangeFunction) (string, error) {
	output := []string{}
	if node.Lateral {
		output = append(output, "LATERAL")
	}
	function, err := c.deparseItem(node.Functions.Items[0].(nodes.List).Items[0]) // FIXME: Needs more test cases
	if err != nil {
		return "", err
	}
	output = append(output, function)
	if node.Alias != nil {
		alias, err := c.deparseItem(node.Alias)
		if err != nil {
			return "", err
		}
		output = append(output, alias)
	}
	if node.Coldeflist.Items != nil {
		coldeflistItems, err := c.deparseItemList(node.Coldeflist)
		if err != nil {
			return "", err
		}
		var coldeflist string
		if node.Alias != nil {
			coldeflist = fmt.Sprintf("(%s)", strings.Join(coldeflistItems, ", "))
		} else {
			coldeflist = fmt.Sprintf("AS (%s)", strings.Join(coldeflistItems, ", "))
		}
		output = append(output, coldeflist)
	}
	return strings.Join(output, " "), nil
}
func (c DeparseContext) deparseRangeSubselect(node nodes.RangeSubselect) (string, error) {
	subquery, err := c.deparseItem(node.Subquery)
	if err != nil {
		return "", err
	}
	output := fmt.Sprintf("(%s)", subquery)
	if node.Alias != nil {
		alias, err := c.deparseItem(node.Alias)
		if err != nil {
			return "", err
		}
		output = fmt.Sprintf("%s %s", output, alias)
	}
	return output, nil
}

func (c DeparseContext) deparseRangeVar(node nodes.RangeVar) (string, error) {
	output := []string{}

	if !node.Inh {
		output = append(output, "ONLY")
	}
	if node.Schemaname != nil {
		output = append(output, fmt.Sprintf(`"%s"."%s"`, *node.Schemaname, *node.Relname))
	} else {
		output = append(output, fmt.Sprintf(`"%s"`, *node.Relname))
	}
	if node.Alias != nil {
		alias, err := c.deparseItem(node.Alias)
		if err != nil {
			return "", err
		}
		output = append(output, alias)
	}
	return strings.Join(output, " "), nil
}

func (c DeparseContext) deparseRawStmt(node nodes.RawStmt) (string, error) {
	return c.deparseItem(node.Stmt)
}

func (c DeparseContext) deparseResTarget(node nodes.ResTarget) (string, error) {
	if c.Context == "select" {
		val, err := c.deparseItem(node.Val)
		if err != nil {
			return "", err
		}
		if node.Name != nil {
			return fmt.Sprintf("%s AS %s", val, *node.Name), nil
		} else {
			return val, nil
		}
	}
	// TODO context == update
	// TODO node.Val == nil
	return "", fmt.Errorf("Can't deparse %# v in context %s", pretty.Formatter(node), c.Context)
}

func (c DeparseContext) deparseRowExpr(node nodes.RowExpr) (string, error) {
	argItems, err := c.deparseItemList(node.Args)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ROW(%s)", strings.Join(argItems, ", ")), nil
}

func (c DeparseContext) deparseSelect(node nodes.SelectStmt) (string, error) {
	output := []string{}
	ctx := DeparseContext{Context: "select"}

	if node.Op == 1 {
		larg, err := ctx.deparseItem(node.Larg)
		if err != nil {
			return "", err
		}
		output = append(output, larg)
		output = append(output, "UNION")
		if node.All {
			output = append(output, "ALL")
		}
		rarg, err := ctx.deparseItem(node.Rarg)
		if err != nil {
			return "", err
		}
		output = append(output, rarg)
		return strings.Join(output, " "), nil
	}

	if node.WithClause != nil {
		withClause, err := ctx.deparseItem(node.WithClause)
		if err != nil {
			return "", err
		}
		output = append(output, withClause)
	}

	if node.TargetList.Items != nil {
		output = append(output, "SELECT")
		if node.DistinctClause.Items != nil {
			output = append(output, "DISTINCT")
		}
		targetListItems, err := ctx.deparseItemList(node.TargetList)
		if err != nil {
			return "", err
		}
		targetList := strings.Join(targetListItems, ", ")
		output = append(output, targetList)
	}

	if node.FromClause.Items != nil {
		output = append(output, "FROM")
		fromClauseItems, err := ctx.deparseItemList(node.FromClause)
		if err != nil {
			return "", err
		}
		fromClause := strings.Join(fromClauseItems, ", ")
		output = append(output, fromClause)
	}

	if node.WhereClause != nil {
		output = append(output, "WHERE")
		whereClause, err := ctx.deparseItem(node.WhereClause)
		if err != nil {
			return "", err
		}
		output = append(output, whereClause)
	}

	if node.ValuesLists != nil {
		output = append(output, "VALUES")
		valuesListsItems := make([]string, len(node.ValuesLists))
		for i, valuesList := range node.ValuesLists {
			valuesItems := make([]string, len(valuesList))
			for j, valuesItem := range valuesList {
				result, err := ctx.deparseItem(valuesItem)
				if err != nil {
					return "", err
				}
				valuesItems[j] = result
			}
			valuesListsItems[i] = fmt.Sprintf("(%s)", strings.Join(valuesItems, ", "))
		}
		output = append(output, strings.Join(valuesListsItems, ", "))
	}

	if node.GroupClause.Items != nil {
		output = append(output, "GROUP BY")
		groupItems, err := ctx.deparseItemList(node.GroupClause)
		if err != nil {
			return "", err
		}
		output = append(output, strings.Join(groupItems, ", "))
	}

	//TODO HavingClause

	if node.SortClause.Items != nil {
		output = append(output, "ORDER BY")
		sortItems, err := ctx.deparseItemList(node.SortClause)
		if err != nil {
			return "", err
		}
		output = append(output, strings.Join(sortItems, ", "))
	}

	if node.LimitCount != nil {
		output = append(output, "LIMIT")
		limitCount, err := ctx.deparseItem(node.LimitCount)
		if err != nil {
			return "", err
		}
		output = append(output, limitCount)
	}

	if node.LimitOffset != nil {
		output = append(output, "OFFSET")
		limitOffset, err := ctx.deparseItem(node.LimitOffset)
		if err != nil {
			return "", err
		}
		output = append(output, limitOffset)
	}

	if node.LockingClause.Items != nil {
		lockingClauseItems, err := ctx.deparseItemList(node.LockingClause)
		if err != nil {
			return "", err
		}
		output = append(output, lockingClauseItems...)
	}

	return strings.Join(output, " "), nil
}

func (c DeparseContext) deparseSortBy(node nodes.SortBy) (string, error) {
	output := []string{}
	result, err := c.deparseItem(node.Node)
	if err != nil {
		return "", err
	}
	output = append(output, result)
	switch node.SortbyDir {
	case nodes.SORTBY_ASC:
		output = append(output, "ASC")
	case nodes.SORTBY_DESC:
		output = append(output, "DESC")
	}

	return strings.Join(output, " "), nil
}

func (c DeparseContext) deparseSubLink(node nodes.SubLink) (string, error) {
	subselect, err := c.deparseItem(node.Subselect)
	if err != nil {
		return "", err
	}
	switch node.SubLinkType {
	case nodes.ANY_SUBLINK:
		testexpr, err := c.deparseItem(node.Testexpr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s IN (%s)", testexpr, subselect), nil
	case nodes.EXISTS_SUBLINK:
		return fmt.Sprintf("EXISTS(%s)", subselect), nil
	default:
		return fmt.Sprintf("(%s)", subselect), nil
	}
}

func (c DeparseContext) deparseTypeCast(node nodes.TypeCast) (string, error) {
	arg, err := c.deparseItem(node.Arg)
	if err != nil {
		return "", err
	}
	ctx := DeparseContext{Context: "type_name"}
	typeName, err := ctx.deparseItem(node.TypeName)
	if err != nil {
		return "", err
	}
	if typeName == "boolean" {
		if arg == "'t'" {
			return "true", nil
		}
		return "false", nil
	}
	return fmt.Sprintf("%s::%s", arg, typeName), nil
}

func (c DeparseContext) deparseTypeName(node nodes.TypeName) (string, error) {
	ctx := DeparseContext{Context: "type_name"}
	nameItems, err := ctx.deparseItemList(node.Names)
	if err != nil {
		return "", err
	}
	// TODO interval

	output := []string{}
	if node.Setof {
		output = append(output, "SETOF")
	}
	var typmods string
	if node.Typmods.Items != nil {
		typmodItems, err := c.deparseItemList(node.Typmods)
		if err != nil {
			return "", err
		}
		typmods = strings.Join(typmodItems, ", ")
	}
	typeNameCast, err := c.deparseTypeNameCast(nameItems, typmods)
	if err != nil {
		return "", err
	}
	output = append(output, typeNameCast)
	if node.ArrayBounds.Items != nil {
		output[len(output)-1] = fmt.Sprintf("%s[]", output[len(output)-1])
	}
	return strings.Join(output, " "), nil
}

func (c DeparseContext) deparseTypeNameCast(names []string, arguments string) (string, error) {
	if names[0] != "pg_catalog" {
		return strings.Join(names, "."), nil
	}
	switch names[1] {
	case "bpchar":
		return fmt.Sprintf("char(%s)", arguments), nil
	case "varchar":
		if arguments != "" {
			return fmt.Sprintf("varchar(%s)", arguments), nil
		}
		return "varchar", nil
	case "numeric":
		if arguments != "" {
			return fmt.Sprintf("numeric(%s)", arguments), nil
		}
		return "numeric", nil
	case "bool":
		return "boolean", nil
	case "int2":
		return "smallint", nil
	case "int4":
		return "int", nil
	case "int8":
		return "bigint", nil
	case "real", "float4":
		return "real", nil
	case "float8":
		return "double", nil
	case "time":
		return "time", nil
	case "timetz":
		return "time with time zone", nil
	case "timestamp":
		return "timestamp", nil
	case "timestamptz":
		return "timestamp with time zone", nil
	default:
		return "", fmt.Errorf("Can't deparse type %s", names[1])
	}
}

func (c DeparseContext) deparseWithClause(node nodes.WithClause) (string, error) {
	output := []string{}
	output = append(output, "WITH")
	if node.Recursive {
		output = append(output, "RECURSIVE")
	}
	cteItems, err := c.deparseItemList(node.Ctes)
	if err != nil {
		return "", err
	}
	output = append(output, strings.Join(cteItems, ", "))
	return strings.Join(output, " "), nil
}

/*
def deparse_windowdef(node)
	return deparse_identifier(node['name']) if node['name']

	output = []

	if node['partitionClause']
		output << 'PARTITION BY'
		output << node['partitionClause'].map do |item|
			deparse_item(item)
		end.join(', ')
	end

	if node['orderClause']
		output << 'ORDER BY'
		output << node['orderClause'].map do |item|
			deparse_item(item)
		end.join(', ')
	end

	format('(%s)', output.join(' '))
end
*/

func (c DeparseContext) deparseWindowDef(node nodes.WindowDef) (string, error) {
	// if len(*node.Name) != 0 {
	// 	return deparseI
	// }

	output := strings.Builder{}

	if len(node.PartitionClause.Items) > 0 {
		output.WriteString("PARTITION BY ")
		cteItems, err := c.deparseItemList(node.PartitionClause)
		if err != nil {
			return "", err
		}
		output.WriteString(strings.Join(cteItems, ", "))
		output.WriteByte(' ')
	}

	if len(node.OrderClause.Items) > 0 {
		output.WriteString("ORDER BY ")
		cteItems, err := c.deparseItemList(node.OrderClause)
		if err != nil {
			return "", err
		}
		output.WriteString(strings.Join(cteItems, ", "))
		output.WriteByte(' ')
	}

	return output.String(), nil
}
