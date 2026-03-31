#pragma once

#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "util.h"

namespace ir_sql_converter {

enum SimplestVarType {
  InvalidVarType = 0,
  BoolVar,
  IntVar,
  FloatVar,
  StringVar,
  StringVarArr,
  Date
};
enum SimplestJoinType {
  InvalidJoinType = 0,
  Inner,
  Left,
  Full,
  Right,
  Mark,
  Semi,
  Anti,
  UniqueOuter,
  UniqueInner
};
enum SimplestLogicalOp {
  InvalidLogicalOp = 0,
  LogicalAnd,
  LogicalOr,
  LogicalNot
};
enum SimplestTextOrder { InvalidTextOrder = 0, DefaultTextOrder, UTF8, C };
enum SimplestAggFnType { InvalidAggType = 0, Min, Max, Sum, Average };
enum SimplestOrderType { INVALID = 0, ORDER_DEFAULT, Ascending, Descending };
enum SimplestLimitType {
  UNSET = 0,
  CONSTANT_VALUE,
  CONSTANT_PERCENTAGE,
  EXPRESSION_VALUE,
  EXPRESSION_PERCENTAGE
};
enum SimplestExprType {
  InvalidExprType = 0,
  Equal,
  NotEqual,
  LessThan,
  GreaterThan,
  LessEqual,
  GreaterEqual,
  NullType,
  NonNullType,
  TextLike,
  Text_Not_Like,
  LogicalOp,
  SingleAttr
};
enum SimplestNodeType {
  InvalidNodeType = 0,
  LiteralNode,
  VarNode,
  ConstVarNode,
  AttrVarNode,
  ParamVarNode,
  ExprNode,
  IsNullExprNode,
  VarComparisonNode,
  VarConstComparisonNode,
  VarParamComparisonNode,
  LogicalExprNode,
  SingleAttrExprNode,
  StmtNode,
  ProjectionNode,
  AggregateNode,
  OrderNode,
  LimitNode,
  JoinNode,
  CrossProductNode,
  FilterNode,
  ScanNode,
  ChunkNode,
  HashNode,
  SortNode,
  RawSQLNode
};

class AQPNode {
public:
  explicit AQPNode(SimplestNodeType node_type) : node_type(node_type) {};

  virtual ~AQPNode() = default;

  template <class TARGET> TARGET &Cast() {
    return reinterpret_cast<TARGET &>(*this);
  }

  template <class TARGET> const TARGET &Cast() const {
    return reinterpret_cast<const TARGET &>(*this);
  }

  virtual std::string Print(bool print = true, int depth = 0) = 0;

  SimplestNodeType GetNodeType() const { return node_type; }

  void ChangeNodeType(SimplestNodeType new_type) { node_type = new_type; }

private:
  SimplestNodeType node_type;
};

class SimplestLiteral : public AQPNode {
public:
  explicit SimplestLiteral(std::string literal_value)
      : AQPNode(LiteralNode), literal_value(std::move(literal_value)) {};

  SimplestLiteral(const SimplestLiteral &other)
      : AQPNode(other.GetNodeType()),
        literal_value(other.literal_value) {};

  ~SimplestLiteral() override = default;

  std::string Print(bool print = true, int depth = 0) override {
    std::string str = " \"" + literal_value + "\" ";
    if (print)
      std::cout << str << std::endl;
    return str;
  };

  std::string GetLiteralValue() { return literal_value; }

private:
  std::string literal_value;
};

using table_str =
    std::unordered_map<std::string,
                       std::vector<std::unique_ptr<SimplestLiteral>>>;

class SimplestVar : public AQPNode {
public:
  SimplestVar(SimplestVarType type, bool is_const, SimplestNodeType node_type)
      : AQPNode(node_type), type(type), is_const(is_const) {};

  SimplestVar(const SimplestVar &other)
      : AQPNode(other.GetNodeType()), type(other.type),
        is_const(other.is_const) {};

  ~SimplestVar() override = default;

  SimplestVarType GetType() const { return type; }

  void ChangeVarType(SimplestVarType new_type) { type = new_type; }

  bool IsConst() { return is_const; }

  std::string Print(bool print = true, int depth = 0) override = 0;

private:
  SimplestVarType type;
  bool is_const;
};

class SimplestConstVar : public SimplestVar {
public:
  explicit SimplestConstVar(bool bool_value)
      : SimplestVar(SimplestVarType::BoolVar, true, ConstVarNode),
        bool_value(bool_value) {};

  explicit SimplestConstVar(int int_value)
      : SimplestVar(SimplestVarType::IntVar, true, ConstVarNode),
        int_value(int_value) {};

  explicit SimplestConstVar(float float_value)
      : SimplestVar(SimplestVarType::FloatVar, true, ConstVarNode),
        float_value(float_value) {};

  explicit SimplestConstVar(std::string str_value)
      : SimplestVar(SimplestVarType::StringVar, true, ConstVarNode),
        str_value(std::move(str_value)) {};

  explicit SimplestConstVar(std::vector<std::string> str_vec_value)
      : SimplestVar(SimplestVarType::StringVarArr, true, ConstVarNode),
        str_vec_value(std::move(str_vec_value)) {};

  explicit SimplestConstVar(const SimplestConstVar &other)
      : SimplestVar(other.GetType(), true, ConstVarNode),
        bool_value(other.bool_value), int_value(other.int_value),
        float_value(other.float_value), str_value(other.str_value) {};

  explicit SimplestConstVar(std::unique_ptr<SimplestConstVar> other)
      : SimplestVar(other->GetType(), true, ConstVarNode),
        bool_value(other->bool_value), int_value(other->int_value),
        float_value(other->float_value), str_value(other->str_value) {};

  ~SimplestConstVar() override = default;

  bool GetBoolValue() const { return bool_value; }

  int GetIntValue() const { return int_value; }

  float GetFloatValue() const { return float_value; }

  std::string GetStringValue() const { return str_value; }

  std::vector<std::string> GetStringVecValue() const { return str_vec_value; }

  std::string Print(bool print = true, int depth = 0) override {
    std::string str;
    switch (GetType()) {
    case InvalidVarType:
      std::cout << "\ninvalid Vary Type!!!" << std::endl;
      return str;
    case BoolVar:
      str = "Bool const value: " + std::to_string(bool_value);
      break;
    case IntVar:
      str = "Integer const value: " + std::to_string(int_value);
      break;
    case FloatVar:
      str = "Float const value: " + std::to_string(float_value);
      break;
    case StringVar:
      str = "String const value: \"" + str_value + "\"";
      break;
    case StringVarArr:
      str = "String const value: [";
      for (const auto &str_val : str_vec_value) {
        str += "\"" + str_val + "\", ";
      }
      str += "]";
      break;
    case Date:
      str = "Data const value: " + std::to_string(int_value);
      break;
    }
    if (print)
      std::cout << str << std::endl;

    return str;
  }

private:
  bool bool_value{};
  int int_value{};
  float float_value{};
  std::string str_value;
  std::vector<std::string> str_vec_value;
};

class SimplestAttr : public SimplestVar {
public:
  SimplestAttr(SimplestVarType var_type, unsigned int table_index,
               unsigned int column_index, std::string column_name)
      : SimplestVar(var_type, false, AttrVarNode), table_index(table_index),
        column_index(column_index), column_name(std::move(column_name)) {};

  SimplestAttr(const SimplestAttr &other)
      : SimplestVar(other.GetType(), false, AttrVarNode),
        table_index(other.table_index), column_index(other.column_index),
        column_name(other.column_name) {};

  explicit SimplestAttr(std::unique_ptr<SimplestAttr> other)
      : SimplestVar(other->GetType(), false, AttrVarNode),
        table_index(other->table_index), column_index(other->column_index) {};

  ~SimplestAttr() override = default;

  unsigned int GetTableIndex() const { return table_index; }

  unsigned int GetColumnIndex() const { return column_index; }

  std::string GetColumnName() const { return column_name; }

  void SetColumnName(std::string col_name) {
    column_name = std::move(col_name);
  }

  std::string Print(bool print = true, int depth = 0) override {
    std::string str;
    switch (GetType()) {
    case InvalidVarType:
      std::cout << "\ninvalid Vary Type!!!" << std::endl;
      return str;
    case BoolVar:
      str = "Bool variable ";
      break;
    case IntVar:
      str = "Integer variable ";
      break;
    case FloatVar:
      str = "Float variable ";
      break;
    case StringVar:
      str = "String variable ";
      break;
    case StringVarArr:
      str = "String variable Array ";
      break;
    case Date:
      str = "Date variable ";
      break;
    }

    std::string info = "#(" + std::to_string(table_index) + ", " +
                       std::to_string(column_index) + "): " + column_name;
    str += info;

    if (print)
      std::cout << str << std::endl;

    return str;
  }

  bool operator==(const SimplestAttr &other) const {
    return GetTableIndex() == other.GetTableIndex() &&
           GetColumnIndex() == other.GetColumnIndex();
  }

private:
  unsigned int table_index;
  unsigned int column_index;
  std::string column_name;
};

struct SimplestAttrHasher {
  size_t operator()(const std::unique_ptr<SimplestAttr> &other) const {
    return std::hash<unsigned int>()(other->GetTableIndex()) ^
           std::hash<unsigned int>()(other->GetColumnIndex());
  }
};

struct SimplestAttrEqual {
  bool operator()(const std::unique_ptr<SimplestAttr> &lhs,
                  const std::unique_ptr<SimplestAttr> &rhs) const {
    return lhs->GetTableIndex() == rhs->GetTableIndex() &&
           lhs->GetColumnIndex() == rhs->GetColumnIndex();
  }
};

class SimplestParam : public SimplestVar {
public:
  SimplestParam(SimplestVarType var_type, unsigned int param_id)
      : SimplestVar(var_type, false, ParamVarNode), param_id(param_id) {};

  ~SimplestParam() override = default;

  unsigned int GetParamId() const { return param_id; }

  std::string Print(bool print = true, int depth = 0) override {
    std::string str;
    switch (GetType()) {
    case InvalidVarType:
      std::cout << "\ninvalid Vary Type!!!" << std::endl;
      return str;
    case BoolVar:
      str = "Bool variable ";
      break;
    case IntVar:
      str = "Integer variable ";
      break;
    case FloatVar:
      str = "Float variable ";
      break;
    case StringVar:
      str = "String variable ";
      break;
    case StringVarArr:
      str = "String variable Array ";
      break;
    case Date:
      str = "Data variable ";
      break;
    }

    std::string info = "#param(" + std::to_string(param_id) + ")";
    str += info;

    if (print)
      std::cout << str << std::endl;

    return str;
  }

private:
  unsigned int param_id;
};

class AQPExpr : public AQPNode {
public:
  AQPExpr(SimplestExprType expr_type, SimplestNodeType node_type)
      : AQPNode(node_type), expr_type(expr_type) {};

  AQPExpr(const AQPExpr &other)
      : AQPExpr(other.expr_type, other.GetNodeType()) {};

  ~AQPExpr() override = default;

  SimplestExprType GetSimplestExprType() const { return expr_type; }

  virtual std::string Print(bool print = true, int depth = 0) override = 0;

private:
  SimplestExprType expr_type;
};

class SimplestVarComparison : public AQPExpr {
public:
  SimplestVarComparison(SimplestExprType comparison_type,
                        std::unique_ptr<SimplestAttr> left_attr,
                        std::unique_ptr<SimplestAttr> right_attr)
      : AQPExpr(comparison_type, VarComparisonNode),
        left_attr(std::move(left_attr)), right_attr(std::move(right_attr)) {};
  SimplestVarComparison(const SimplestVarComparison &) =
      delete; // no copy constructor
  SimplestVarComparison &
  operator=(const SimplestVarComparison &) = delete; // no copy assignment

  SimplestVarComparison(SimplestVarComparison &&) = default;
  SimplestVarComparison &operator=(SimplestVarComparison &&) = default;

  ~SimplestVarComparison() override = default;

  std::string Print(bool print = true, int depth = 0) override {
    std::string str;
    std::string comparison_op;
    switch (GetSimplestExprType()) {
    case InvalidExprType:
      std::cout << "Invalid Comparison Type!!!" << std::endl;
      return str;
    case Equal:
      comparison_op = " = ";
      break;
    case TextLike:
      comparison_op = " ~~ ";
      break;
    case Text_Not_Like:
      comparison_op = " !~~ ";
      break;
    case LessThan:
      comparison_op = " < ";
      break;
    case GreaterThan:
      comparison_op = " > ";
      break;
    case LessEqual:
      comparison_op = " <= ";
      break;
    case GreaterEqual:
      comparison_op = " >= ";
      break;
    case NotEqual:
      comparison_op = " != ";
      break;
    case NullType:
    case NonNullType:
    case LogicalOp:
    case SingleAttr:
      std::cout << "This should be a `SimplestVarComparison`!!!" << std::endl;
      return str;
    }

    str = left_attr->Print(false);
    str += comparison_op;
    str += right_attr->Print(false);

    if (print)
      std::cout << str << std::endl;

    return str;
  }

  std::unique_ptr<SimplestAttr> left_attr;
  std::unique_ptr<SimplestAttr> right_attr;
};

class SimplestVarParamComparison : public AQPExpr {
public:
  SimplestVarParamComparison(SimplestExprType comparison_type,
                             std::unique_ptr<SimplestAttr> attr,
                             std::unique_ptr<SimplestParam> param_var)
      : AQPExpr(comparison_type, VarParamComparisonNode),
        attr(std::move(attr)), param_var(std::move(param_var)) {};

  SimplestVarParamComparison(const SimplestVarParamComparison &) =
      delete; // no copy constructor
  SimplestVarParamComparison &
  operator=(const SimplestVarParamComparison &) = delete; // no copy assignment

  SimplestVarParamComparison(SimplestVarParamComparison &&) = default;
  SimplestVarParamComparison &
  operator=(SimplestVarParamComparison &&) = default;

  ~SimplestVarParamComparison() override = default;

  std::string Print(bool print = true, int depth = 0) override {
    std::string str;
    std::string comparison_op;
    switch (GetSimplestExprType()) {
    case InvalidExprType:
      std::cout << "Invalid Comparison Type!!!" << std::endl;
      return str;
    case Equal:
      comparison_op = " = ";
      break;
    case TextLike:
      comparison_op = " ~~ ";
      break;
    case Text_Not_Like:
      comparison_op = " !~~ ";
      break;
    case LessThan:
      comparison_op = " < ";
      break;
    case GreaterThan:
      comparison_op = " > ";
      break;
    case LessEqual:
      comparison_op = " <= ";
      break;
    case GreaterEqual:
      comparison_op = " >= ";
      break;
    case NotEqual:
      comparison_op = " != ";
      break;
    case NullType:
    case NonNullType:
    case LogicalOp:
    case SingleAttr:
      std::cout << "This should be a `SimplestVarParamComparison`!!!"
                << std::endl;
      return str;
    }

    str = attr->Print(false);
    str += comparison_op;
    str += param_var->Print(false);

    if (print)
      std::cout << str << std::endl;

    return str;
  }

  std::unique_ptr<SimplestAttr> attr;
  std::unique_ptr<SimplestParam> param_var;
};

class SimplestVarConstComparison : public AQPExpr {
public:
  SimplestVarConstComparison(SimplestExprType comparison_type,
                             std::unique_ptr<SimplestAttr> attr,
                             std::unique_ptr<SimplestConstVar> const_var)
      : AQPExpr(comparison_type, VarConstComparisonNode),
        attr(std::move(attr)), const_var(std::move(const_var)) {};

  SimplestVarConstComparison(const SimplestVarConstComparison &) =
      delete; // no copy constructor
  SimplestVarConstComparison &
  operator=(const SimplestVarConstComparison &) = delete; // no copy assignment

  SimplestVarConstComparison(SimplestVarConstComparison &&) = default;
  SimplestVarConstComparison &
  operator=(SimplestVarConstComparison &&) = default;

  ~SimplestVarConstComparison() override = default;

  std::string Print(bool print = true, int depth = 0) override {
    std::string str;
    std::string comparison_op;
    switch (GetSimplestExprType()) {
    case InvalidExprType:
      std::cout << "Invalid Comparison Type!!!" << std::endl;
      return str;
    case Equal:
      comparison_op = " = ";
      break;
    case TextLike:
      comparison_op = " ~~ ";
      break;
    case Text_Not_Like:
      comparison_op = " !~~ ";
      break;
    case LessThan:
      comparison_op = " < ";
      break;
    case GreaterThan:
      comparison_op = " > ";
      break;
    case LessEqual:
      comparison_op = " <= ";
      break;
    case GreaterEqual:
      comparison_op = " >= ";
      break;
    case NotEqual:
      comparison_op = " != ";
      break;
    case NullType:
    case NonNullType:
    case LogicalOp:
    case SingleAttr:
      std::cout << "This should be a `SimplestVarConstComparison`!!!"
                << std::endl;
      return str;
    }

    str = attr->Print(false);
    str += comparison_op;
    str += const_var->Print(false);

    if (print)
      std::cout << str << std::endl;

    return str;
  }

  std::unique_ptr<SimplestAttr> attr;
  std::unique_ptr<SimplestConstVar> const_var;
};

class SimplestIsNullExpr : public AQPExpr {
public:
  SimplestIsNullExpr(SimplestExprType is_null_type,
                     std::unique_ptr<SimplestAttr> attr)
      : AQPExpr(is_null_type, IsNullExprNode), attr(std::move(attr)) {};

  SimplestIsNullExpr(const SimplestIsNullExpr &) =
      delete; // no copy constructor
  SimplestIsNullExpr &
  operator=(const SimplestIsNullExpr &) = delete; // no copy assignment

  SimplestIsNullExpr(SimplestIsNullExpr &&) = default;
  SimplestIsNullExpr &operator=(SimplestIsNullExpr &&) = default;

  ~SimplestIsNullExpr() override = default;

  std::string Print(bool print = true, int depth = 0) override {
    std::string str;
    std::string is_null_op;
    switch (GetSimplestExprType()) {
    case InvalidExprType:
      std::cout << "Invalid expr Type!!!" << std::endl;
      return str;
    case Equal:
    case TextLike:
    case Text_Not_Like:
    case LessThan:
    case GreaterThan:
    case LessEqual:
    case GreaterEqual:
    case NotEqual:
    case LogicalOp:
    case SingleAttr:
      std::cout << "This should not be a `SimplestIsNullExpr`!!!" << std::endl;
      return str;
    case NullType:
      is_null_op = " is null";
      break;
    case NonNullType:
      is_null_op = " is not null";
      break;
    }

    str = attr->Print(false);
    str += is_null_op;

    if (print)
      std::cout << str << std::endl;

    return str;
  }

  std::unique_ptr<SimplestAttr> attr;
};

// if it's a logical not, let `left_expr` be nullptr
class SimplestLogicalExpr : public AQPExpr {
public:
  // todo: here might be more than two children?
  SimplestLogicalExpr(SimplestLogicalOp logical_op,
                      std::unique_ptr<AQPExpr> left_expr,
                      std::unique_ptr<AQPExpr> right_expr)
      : AQPExpr(LogicalOp, LogicalExprNode),
        left_expr(std::move(left_expr)), right_expr(std::move(right_expr)),
        logical_op(logical_op) {};

  SimplestLogicalExpr(const SimplestLogicalExpr &) =
      delete; // no copy constructor
  SimplestLogicalExpr &
  operator=(const SimplestLogicalExpr &) = delete; // no copy assignment

  SimplestLogicalExpr(SimplestLogicalExpr &&) = default;
  SimplestLogicalExpr &operator=(SimplestLogicalExpr &&) = default;

  ~SimplestLogicalExpr() override = default;

  SimplestLogicalOp GetLogicalOp() const { return logical_op; }

  std::string Print(bool print = true, int depth = 0) override {
    std::string str;
    std::string logical_op_str;
    switch (GetLogicalOp()) {
    case InvalidLogicalOp:
      std::cout << "Invalid expr Type!!!" << std::endl;
      return str;
    case LogicalAnd:
      logical_op_str = " && ";
      break;
    case LogicalOr:
      logical_op_str = " || ";
      break;
    case LogicalNot:
      logical_op_str = "!";
      assert(nullptr == left_expr);
      break;
    }

    if (LogicalNot != GetLogicalOp()) {
      str += left_expr->Print(false);
    }
    str += logical_op_str;
    str += right_expr->Print(false);

    if (print)
      std::cout << str << std::endl;

    return str;
  }

  std::unique_ptr<AQPExpr> left_expr;
  std::unique_ptr<AQPExpr> right_expr;

private:
  SimplestLogicalOp logical_op;
};

// it can be a single attr, e.g. the `In` clause of the Filter node
class SimplestSingleAttrExpr : public AQPExpr {
public:
  explicit SimplestSingleAttrExpr(std::unique_ptr<SimplestAttr> attr)
      : AQPExpr(SimplestExprType::SingleAttr, SingleAttrExprNode),
        attr(std::move(attr)) {};

  SimplestSingleAttrExpr(const SimplestSingleAttrExpr &) =
      delete; // no copy constructor
  SimplestSingleAttrExpr &
  operator=(const SimplestSingleAttrExpr &) = delete; // no copy assignment

  SimplestSingleAttrExpr(SimplestSingleAttrExpr &&) = default;
  SimplestSingleAttrExpr &operator=(SimplestSingleAttrExpr &&) = default;

  ~SimplestSingleAttrExpr() override = default;

  std::string Print(bool print = true, int depth = 0) override {
    std::string str;
    str = "SingleAttrExpr: ";
    str += attr->Print(false);

    if (print)
      std::cout << str << std::endl;

    return str;
  }

  std::unique_ptr<SimplestAttr> attr;
};

class AQPStmt : public AQPNode {
public:
  AQPStmt(std::vector<std::unique_ptr<AQPStmt>> children,
               std::vector<std::unique_ptr<SimplestAttr>> target_list,
               std::vector<std::unique_ptr<AQPExpr>> qual_vec,
               SimplestNodeType node_type)
      : AQPNode(node_type), target_list(std::move(target_list)),
        children(std::move(children)), qual_vec(std::move(qual_vec)) {};

  AQPStmt(std::vector<std::unique_ptr<SimplestAttr>> target_list,
               std::vector<std::unique_ptr<AQPExpr>> qual_vec,
               SimplestNodeType node_type)
      : AQPNode(node_type), target_list(std::move(target_list)),
        qual_vec(std::move(qual_vec)) {};

  AQPStmt(std::vector<std::unique_ptr<AQPStmt>> children,
               std::vector<std::unique_ptr<SimplestAttr>> target_list,
               SimplestNodeType node_type)
      : AQPNode(node_type), children(std::move(children)),
        target_list(std::move(target_list)) {};

  AQPStmt(std::vector<std::unique_ptr<AQPStmt>> children,
               SimplestNodeType node_type)
      : AQPNode(node_type), children(std::move(children)) {};

  AQPStmt(std::unique_ptr<AQPStmt> stmt, SimplestNodeType node_type)
      : AQPNode(node_type), target_list(std::move(stmt->target_list)),
        children(std::move(stmt->children)),
        qual_vec(std::move(stmt->qual_vec)),
        estimated_cardinality(stmt->estimated_cardinality) {};

  AQPStmt(const AQPStmt &) = delete;            // no copy constructor
  AQPStmt &operator=(const AQPStmt &) = delete; // no copy assignment

  AQPStmt(AQPStmt &&) = default;
  AQPStmt &operator=(AQPStmt &&) = default;

  ~AQPStmt() override = default;

  void SimplestAddChild(std::unique_ptr<AQPStmt> child) {
    children.emplace_back(std::move(child));
  }

  uint64_t GetEstimatedCardinality() const { return estimated_cardinality; }
  void SetEstimatedCardinality(uint64_t card) { estimated_cardinality = card; }

  std::string Print(bool print = true, int depth = 0) override {
    std::string str;
    std::string indent(depth * 2, ' ');

    if (!qual_vec.empty()) {
      str += " [";
      for (const auto &qual : qual_vec) {
        str += qual->Print(false);
      }
      str += "]";
    }

    for (size_t i = 0; i < children.size(); i++) {
      if (children[i]) {
        str += "\n" + indent + "  -> ";
        str += children[i]->Print(false, depth + 1);
      }
    }

    return str;
  };

  std::vector<std::unique_ptr<SimplestAttr>> target_list;

  // children[0] is the left node, children[1] is the right node
  std::vector<std::unique_ptr<AQPStmt>> children;

  // implicitly condition - from postgres
  // todo: need to check if only var-const comparison exists
  std::vector<std::unique_ptr<AQPExpr>> qual_vec;

protected:
  uint64_t estimated_cardinality = 0;
};

class SimplestProjection : public AQPStmt {
public:
  SimplestProjection(std::unique_ptr<AQPStmt> base_stmt,
                     unsigned int table_index)
      : AQPStmt(std::move(base_stmt), ProjectionNode),
        table_index(table_index) {};

  ~SimplestProjection() override = default;

  unsigned int GetIndex() const { return table_index; }

  std::string Print(bool print = true, int depth = 0) override {
    std::string str = "Projection";
    if (depth == 0 && !target_list.empty()) {
      str += " [";
      bool first = true;
      for (const auto &t : target_list) {
        if (!first)
          str += ", ";
        str += t->Print(false);
        first = false;
      }
      str += "]";
    }
    str += AQPStmt::Print(false, depth);

    if (print)
      std::cout << str << std::endl;

    return str;
  }

private:
  unsigned int table_index;
};

using agg_fn_pair =
    std::vector<std::pair<std::unique_ptr<SimplestAttr>, SimplestAggFnType>>;

class SimplestAggregate : public AQPStmt {
public:
  SimplestAggregate(std::unique_ptr<AQPStmt> base_stmt,
                    agg_fn_pair agg_fns)
      : AQPStmt(std::move(base_stmt), AggregateNode),
        agg_fns(std::move(agg_fns)) {};

  SimplestAggregate(std::unique_ptr<AQPStmt> base_stmt,
                    agg_fn_pair agg_fns, unsigned int agg_index,
                    unsigned int group_index)
      : AQPStmt(std::move(base_stmt), AggregateNode),
        agg_fns(std::move(agg_fns)), agg_index(agg_index),
        group_index(group_index) {};

  SimplestAggregate(std::unique_ptr<AQPStmt> base_stmt,
                    agg_fn_pair agg_fns,
                    std::vector<std::unique_ptr<SimplestAttr>> groups,
                    unsigned int agg_index, unsigned int group_index)
      : AQPStmt(std::move(base_stmt), AggregateNode),
        agg_fns(std::move(agg_fns)), groups(std::move(groups)),
        agg_index(agg_index), group_index(group_index) {};

  SimplestAggregate(const SimplestAggregate &) = delete; // no copy constructor
  SimplestAggregate &
  operator=(const SimplestAggregate &) = delete; // no copy assignment

  SimplestAggregate(SimplestAggregate &&) = default;
  SimplestAggregate &operator=(SimplestAggregate &&) = default;

  ~SimplestAggregate() override = default;

  std::string Print(bool print = true, int depth = 0) override {
    std::string str = "Aggregate";

    str += " [";
    bool first = true;
    for (const auto &agg_fn : agg_fns) {
      if (!first)
        str += ", ";
      first = false;
      switch (agg_fn.second) {
      case SimplestAggFnType::InvalidAggType:
        std::cout << "Invalid expr Type!!!" << std::endl;
        return str;
      case SimplestAggFnType::Min:
        str += "min(";
        break;
      case SimplestAggFnType::Max:
        str += "max(";
        break;
      case SimplestAggFnType::Sum:
        str += "sum(";
        break;
      case SimplestAggFnType::Average:
        str += "avg(";
        break;
      }
      str += agg_fn.first->Print(false);
      str += ")";
    }
    str += "]";

    if (!groups.empty()) {
      str += " Group By [";
      bool first_group = true;
      for (const auto &group : groups) {
        if (!first_group)
          str += ", ";
        first_group = false;
        str += group->Print(false);
      }
      str += "]";
    }

    str += AQPStmt::Print(false, depth);

    if (print)
      std::cout << str << std::endl;

    return str;
  }

  unsigned int GetAggIndex() const { return agg_index; }

  unsigned int GetGroupIndex() const { return group_index; }

  agg_fn_pair agg_fns;
  std::vector<std::unique_ptr<SimplestAttr>> groups;

private:
  //! these are only used for DuckDB
  unsigned int agg_index{};
  unsigned int group_index{};
};

struct OrderStruct {
  SimplestOrderType order_type;
  std::unique_ptr<SimplestAttr> attr;
};

class SimplestOrderBy : public AQPStmt {
public:
  SimplestOrderBy(std::unique_ptr<AQPStmt> base_stmt,
                  std::vector<OrderStruct> orders)
      : AQPStmt(std::move(base_stmt), OrderNode),
        orders(std::move(orders)) {};

  ~SimplestOrderBy() override = default;

  std::string Print(bool print = true, int depth = 0) override {
    std::string str = "\n";

    std::string order_str = "\n";
    for (const auto &order : orders) {
      order_str += order.attr->Print(false);
      switch (order.order_type) {
      case SimplestOrderType::INVALID:
        std::cout << "Invalid order Type!!!" << std::endl;
        return str;
      case SimplestOrderType::ORDER_DEFAULT:
        break;
      case SimplestOrderType::Ascending:
        order_str += " ascending";
        break;
      case SimplestOrderType::Descending:
        order_str += " descending";
        break;
      }
      order_str += "\n";
    }
    str += "Order By:\n";
    str += order_str;

    str += AQPStmt::Print(false, depth);

    if (print)
      std::cout << str << std::endl;

    return str;
  }

  std::vector<idx_t> projections;
  std::vector<OrderStruct> orders;
};

struct LimitVal {
  SimplestLimitType type;
  idx_t val;
};

class SimplestLimit : public AQPStmt {
public:
  SimplestLimit(std::unique_ptr<AQPStmt> base_stmt, LimitVal limit_val,
                LimitVal offset_val)
      : AQPStmt(std::move(base_stmt), LimitNode), limit_val(limit_val),
        offset_val(offset_val) {};

  ~SimplestLimit() override = default;

  std::string Print(bool print = true, int depth = 0) override {
    std::string str = "\n";

    std::string limit_str = "\n";
    switch (limit_val.type) {
    case SimplestLimitType::UNSET:
      std::cout << "unset limit type!!!" << std::endl;
      return str;
    case SimplestLimitType::CONSTANT_VALUE:
      limit_str += std::to_string(limit_val.val);
      break;
    default:
      std::cout << "Unsupport limit type!!!" << std::endl;
      assert(false);
    }

    if (offset_val.type != SimplestLimitType::UNSET) {
      limit_str += " ";
      switch (offset_val.type) {
      case SimplestLimitType::UNSET:
        std::cout << "unset limit type!!!" << std::endl;
        return str;
      case SimplestLimitType::CONSTANT_VALUE:
        limit_str += std::to_string(offset_val.val);
        break;
      default:
        std::cout << "Unsupport limit type!!!" << std::endl;
        assert(false);
      }
    }

    str += "Limit:\n";
    str += limit_str;

    str += AQPStmt::Print(false, depth);

    if (print)
      std::cout << str << std::endl;

    return str;
  }

  LimitVal limit_val;
  LimitVal offset_val;
};

class SimplestJoin : public AQPStmt {
public:
  SimplestJoin(
      std::unique_ptr<AQPStmt> base_stmt,
      std::vector<std::unique_ptr<SimplestVarComparison>> join_conditions,
      SimplestJoinType join_type)
      : AQPStmt(std::move(base_stmt), JoinNode),
        join_conditions(std::move(join_conditions)), join_type(join_type) {};

  SimplestJoin(std::unique_ptr<AQPStmt> base_stmt,
               SimplestJoinType join_type)
      : AQPStmt(std::move(base_stmt), JoinNode), join_type(join_type) {};

  SimplestJoin(const SimplestJoin &) = delete;            // no copy constructor
  SimplestJoin &operator=(const SimplestJoin &) = delete; // no copy assignment

  SimplestJoin(SimplestJoin &&) = default;
  SimplestJoin &operator=(SimplestJoin &&) = default;

  ~SimplestJoin() override = default;

  SimplestJoinType GetSimplestJoinType() const { return join_type; }

  void AddJoinCondition(std::vector<std::unique_ptr<SimplestVarComparison>>

                            conds) {
    for (auto &cond : conds) {
      join_conditions.emplace_back(std::move(cond));
    }
  }

  void SetJoinType(SimplestJoinType type) { join_type = type; }

  unsigned int GetMarkIndex() const { return mark_index; }
  void SetMarkIndex(unsigned int idx) { mark_index = idx; }

  std::string Print(bool print = true, int depth = 0) override {
    std::string str;

    switch (join_type) {
    case InvalidJoinType:
      std::cout << "Invalid Join Type!!!" << std::endl;
      return str;
    case Inner:
      str += "Inner";
      break;
    case Left:
      str += "Left";
      break;
    case Full:
      str += "Full";
      break;
    case Right:
      str += "Right";
      break;
    case Mark:
      str += "Mark";
      break;
    case Semi:
      str += "Semi";
      break;
    case Anti:
      str += "Anti";
      break;
    case UniqueOuter:
      str += "UniqueOuter";
      break;
    case UniqueInner:
      str += "UniqueInner";
      break;
    }
    str += " Join";

    if (!join_conditions.empty()) {
      str += " [";
      bool first = true;
      for (const auto &cond : join_conditions) {
        if (!first)
          str += "; ";
        str += cond->Print(false);
        first = false;
      }
      str += "]";
    }

    str += AQPStmt::Print(false, depth);

    if (print)
      std::cout << str << std::endl;

    return str;
  }

  std::vector<std::unique_ptr<SimplestVarComparison>> join_conditions;

private:
  SimplestJoinType join_type;
  unsigned int mark_index{};
};

class SimplestCrossProduct : public AQPStmt {
public:
  explicit SimplestCrossProduct(std::unique_ptr<AQPStmt> base_stmt)
      : AQPStmt(std::move(base_stmt), CrossProductNode) {};

  ~SimplestCrossProduct() override = default;

  std::string Print(bool print = true, int depth = 0) override {
    std::string str = "CrossProduct";

    str += AQPStmt::Print(false, depth);

    if (print)
      std::cout << str << std::endl;

    return str;
  }
};

class SimplestFilter : public AQPStmt {
public:
  explicit SimplestFilter(std::unique_ptr<AQPStmt> base_stmt)
      : AQPStmt(std::move(base_stmt), FilterNode) {};

  //	SimplestFilter(std::unique_ptr<AQPStmt> base_stmt,
  // std::vector<std::unique_ptr<AQPExpr>> filter_conditions) 	    :
  // AQPStmt(std::move(base_stmt), FilterNode),
  // filter_conditions(std::move(filter_conditions)) {};

  //		SimplestFilter(const SimplestFilter&) = delete; // no copy
  // constructor 		SimplestFilter &operator=(const SimplestFilter&)
  // = delete; // no copy assignment
  //
  //		SimplestFilter(SimplestFilter&&) = default;
  //		SimplestFilter &operator=(SimplestFilter&&) = default;

  ~SimplestFilter() override = default;

  std::string Print(bool print = true, int depth = 0) override {
    std::string str = "Filter";
    str += AQPStmt::Print(false, depth);

    if (print)
      std::cout << str << std::endl;

    return str;
  }

  // fixme: is this the same as `qual_vec`?
  //	std::vector<std::unique_ptr<AQPExpr>> filter_conditions;
};

class SimplestScan : public AQPStmt {
public:
  SimplestScan(std::unique_ptr<AQPStmt> base_stmt,
               unsigned int table_index, std::string table_name)
      : AQPStmt(std::move(base_stmt), ScanNode), table_index(table_index),
        table_name(table_name) {};

  ~SimplestScan() override = default;

  unsigned int GetTableIndex() const { return table_index; }

  std::string GetTableName() const { return table_name; }

  void SetTableName(std::string tbl_name) { table_name = std::move(tbl_name); }

  std::string Print(bool print = true, int depth = 0) override {
    std::string str = "Table Scan \"" + table_name +
                      "\" (index=" + std::to_string(table_index) + ")";

    if (print)
      std::cout << str << std::endl;

    return str;
  }

private:
  unsigned int table_index;
  std::string table_name;
};

/*!
 * E.g. The `In` claude from JOB 6d.sql*/
class SimplestChunk : public AQPStmt {
public:
  SimplestChunk(std::unique_ptr<AQPStmt> base_stmt,
                unsigned int table_index, std::vector<std::string> contents)
      : AQPStmt(std::move(base_stmt), ChunkNode), table_index(table_index),
        contents(contents) {};
  SimplestChunk(std::unique_ptr<AQPStmt> base_stmt,
                unsigned int table_index, std::string chunk_name,
                std::vector<std::string> contents)
      : AQPStmt(std::move(base_stmt), ChunkNode), table_index(table_index),
        chunk_name(chunk_name), contents(contents) {};

  ~SimplestChunk() override = default;

  unsigned int GetTableIndex() const { return table_index; }

  std::string GetChunkName() const { return chunk_name; }
  void SetChunkName(std::string name) { chunk_name = name; }

  std::vector<std::string> GetContents() const { return contents; }

  std::string Print(bool print = true, int depth = 0) override {
    std::string str = "Data Chunk ";
    if (!chunk_name.empty()) {
      str += "\"" + chunk_name + "\" ";
    }
    str += "(index=" + std::to_string(table_index) + ") ";
    str += "(card=" + std::to_string(GetEstimatedCardinality()) + ") ";
    if (contents.size() > 10) {
      str += "\ncontent[";
      for (size_t i = 0; i < 10; i++) {
        str += contents[i];
        str += ", ";
      }
      str += "...";
      str += "]";
    } else if (!contents.empty()) {
      str += "\ncontent[";
      for (const auto &content : contents) {
        str += content;
        str += ", ";
      }
      str.erase(str.size() - 2);
      str += "]";
    }

    if (print)
      std::cout << str << std::endl;

    return str;
  }

private:
  // todo: there might be other types
  unsigned int table_index;
  std::vector<std::string> contents;
  std::string chunk_name;
};

class SimplestHash : public AQPStmt {
public:
  SimplestHash(std::unique_ptr<AQPStmt> base_stmt,
               std::vector<std::unique_ptr<SimplestAttr>> hash_keys)
      : AQPStmt(std::move(base_stmt), HashNode),
        hash_keys(std::move(hash_keys)) {};

  SimplestHash(const SimplestHash &) = delete;            // no copy constructor
  SimplestHash &operator=(const SimplestHash &) = delete; // no copy assignment

  SimplestHash(SimplestHash &&) = default;
  SimplestHash &operator=(SimplestHash &&) = default;

  ~SimplestHash() override = default;

  std::string Print(bool print = true, int depth = 0) override {
    std::string str = "\n";
    str += "╔══════════════════╗\n";

    str += "Hash:\nHash Keys:";
    for (const auto &hk : hash_keys) {
      str += "\n" + hk->Print(false);
    }

    str += AQPStmt::Print(false, depth);
    str += "╚══════════════════╝\n";

    if (print)
      std::cout << str << std::endl;

    return str;
  }

  std::vector<std::unique_ptr<SimplestAttr>> hash_keys;
};

struct SimplestOrderStruct {
  SimplestExprType order_type;
  bool nulls_first;
  SimplestTextOrder text_order;
  // start from 1
  int sort_col_idx;
};

//! The `order by` clause
class SimplestSort : public AQPStmt {
public:
  SimplestSort(std::unique_ptr<AQPStmt> base_stmt,
               std::vector<SimplestOrderStruct> order_struct_vec)
      : AQPStmt(std::move(base_stmt), SortNode),
        order_struct_vec(order_struct_vec) {};

  ~SimplestSort() override = default;

  std::vector<SimplestOrderStruct> GetOrderStructVec() const {
    return order_struct_vec;
  };

  std::string Print(bool print = true, int depth = 0) override {
    std::string str = "\n";
    str += "╔══════════════════╗\n";

    str += "Sort:\n";
    for (auto order_struct : order_struct_vec) {
      switch (order_struct.order_type) {
      case InvalidExprType:
        std::cout << "Invalid Order Type!!!" << std::endl;
        return str;
      case LessThan:
        str += "Ascending Order, ";
        break;
      case GreaterThan:
        str += "Descending Order, ";
        break;
      default:
        std::cout << "Doesn't support order type " +
                         std::to_string(order_struct.order_type) + " yet!"
                  << std::endl;
        return str;
      }

      if (order_struct.nulls_first)
        str += "Null First, ";
      else
        str += "Null Last, ";

      switch (order_struct.text_order) {
      case InvalidTextOrder:
        std::cout << "Invalid TextOrder!!!" << std::endl;
        return str;
      case DefaultTextOrder:
        str += "Default Text Order, ";
        break;
      case UTF8:
        str += "UTF8, ";
        break;
      case C:
        str += "C, ";
        break;
      }

      str += "with column " + std::to_string(order_struct.sort_col_idx);
      str += "\n";
    }

    str += AQPStmt::Print(false, depth);
    str += "╚══════════════════╝\n";

    if (print)
      std::cout << str << std::endl;

    return str;
  }

private:
  std::vector<SimplestOrderStruct> order_struct_vec;
};
// Holds a verbatim SQL fragment emitted as-is by ir_to_sql.cpp.
// Used by TensorDagSplitter to wrap tensor op SQL templates so they pass
// through the IR layer without requiring full expression tree construction.
class SimplestRawSQL : public AQPStmt {
public:
  explicit SimplestRawSQL(std::string sql)
      : AQPStmt(std::vector<std::unique_ptr<AQPStmt>>{}, RawSQLNode), raw_sql(std::move(sql)) {}

  ~SimplestRawSQL() override = default;

  const std::string &GetRawSQL() const { return raw_sql; }

  std::string Print(bool print = true, int depth = 0) override {
    std::string str = "RawSQL: " + raw_sql;
    if (print)
      std::cout << str << std::endl;
    return str;
  }

private:
  std::string raw_sql;
};

} // namespace ir_sql_converter
