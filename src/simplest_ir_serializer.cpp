#include "cpp_interface.h"
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace ir_sql_converter {

// Forward declarations for serialization helpers
static void SerializeStmt(std::ostream &out, const SimplestStmt *stmt);
static void SerializeExpr(std::ostream &out, const SimplestExpr *expr);
static void SerializeAttr(std::ostream &out, const SimplestAttr *attr);
static void SerializeConstVar(std::ostream &out,
                              const SimplestConstVar *const_var);

static std::unique_ptr<SimplestStmt> DeserializeStmt(std::istream &in);
static std::unique_ptr<SimplestExpr> DeserializeExpr(std::istream &in);
static std::unique_ptr<SimplestAttr> DeserializeAttr(std::istream &in);
static std::unique_ptr<SimplestConstVar> DeserializeConstVar(std::istream &in);

// Helper to write a string with length prefix
static void WriteString(std::ostream &out, const std::string &str) {
  size_t len = str.size();
  out.write(reinterpret_cast<const char *>(&len), sizeof(len));
  out.write(str.c_str(), len);
}

// Helper to read a string with length prefix
static std::string ReadString(std::istream &in) {
  size_t len;
  in.read(reinterpret_cast<char *>(&len), sizeof(len));
  std::string str(len, '\0');
  in.read(&str[0], len);
  return str;
}

// Serialize SimplestConstVar
static void SerializeConstVar(std::ostream &out,
                              const SimplestConstVar *const_var) {
  if (!const_var) {
    SimplestVarType null_type = InvalidVarType;
    out.write(reinterpret_cast<const char *>(&null_type), sizeof(null_type));
    return;
  }

  SimplestVarType type = const_var->GetType();
  out.write(reinterpret_cast<const char *>(&type), sizeof(type));

  switch (type) {
  case IntVar: {
    int64_t val = const_var->GetIntValue();
    out.write(reinterpret_cast<const char *>(&val), sizeof(val));
    break;
  }
  case FloatVar: {
    double val = const_var->GetFloatValue();
    out.write(reinterpret_cast<const char *>(&val), sizeof(val));
    break;
  }
  case BoolVar: {
    bool val = const_var->GetBoolValue();
    out.write(reinterpret_cast<const char *>(&val), sizeof(val));
    break;
  }
  case StringVar: {
    WriteString(out, const_var->GetStringValue());
    break;
  }
  default:
    throw std::runtime_error("Unknown SimplestVarType");
  }
}

// Deserialize SimplestConstVar
static std::unique_ptr<SimplestConstVar> DeserializeConstVar(std::istream &in) {
  SimplestVarType type;
  in.read(reinterpret_cast<char *>(&type), sizeof(type));

  if (type == InvalidVarType) {
    return nullptr;
  }

  switch (type) {
  case IntVar: {
    int val;
    in.read(reinterpret_cast<char *>(&val), sizeof(val));
    return std::make_unique<SimplestConstVar>(val);
  }
  case FloatVar: {
    float val;
    in.read(reinterpret_cast<char *>(&val), sizeof(val));
    return std::make_unique<SimplestConstVar>(val);
  }
  case BoolVar: {
    bool val;
    in.read(reinterpret_cast<char *>(&val), sizeof(val));
    return std::make_unique<SimplestConstVar>(val);
  }
  case StringVar: {
    std::string val = ReadString(in);
    return std::make_unique<SimplestConstVar>(val);
  }
  default:
    throw std::runtime_error("Unknown SimplestVarType during deserialization");
  }
}

// Serialize SimplestAttr
static void SerializeAttr(std::ostream &out, const SimplestAttr *attr) {
  if (!attr) {
    SimplestVarType null_type = InvalidVarType;
    out.write(reinterpret_cast<const char *>(&null_type), sizeof(null_type));
    return;
  }

  SimplestVarType type = attr->GetType();
  out.write(reinterpret_cast<const char *>(&type), sizeof(type));

  unsigned int table_idx = attr->GetTableIndex();
  unsigned int col_idx = attr->GetColumnIndex();
  out.write(reinterpret_cast<const char *>(&table_idx), sizeof(table_idx));
  out.write(reinterpret_cast<const char *>(&col_idx), sizeof(col_idx));
  WriteString(out, attr->GetColumnName());
}

// Deserialize SimplestAttr
static std::unique_ptr<SimplestAttr> DeserializeAttr(std::istream &in) {
  SimplestVarType type;
  in.read(reinterpret_cast<char *>(&type), sizeof(type));

  if (type == InvalidVarType) {
    return nullptr;
  }

  unsigned int table_idx, col_idx;
  in.read(reinterpret_cast<char *>(&table_idx), sizeof(table_idx));
  in.read(reinterpret_cast<char *>(&col_idx), sizeof(col_idx));
  std::string col_name = ReadString(in);

  return std::make_unique<SimplestAttr>(type, table_idx, col_idx, col_name);
}

// Serialize SimplestExpr
static void SerializeExpr(std::ostream &out, const SimplestExpr *expr) {
  if (!expr) {
    SimplestNodeType null_type = InvalidNodeType;
    out.write(reinterpret_cast<const char *>(&null_type), sizeof(null_type));
    return;
  }

  SimplestNodeType node_type = expr->GetNodeType();
  out.write(reinterpret_cast<const char *>(&node_type), sizeof(node_type));

  SimplestExprType expr_type = expr->GetSimplestExprType();
  out.write(reinterpret_cast<const char *>(&expr_type), sizeof(expr_type));

  switch (node_type) {
  case VarConstComparisonNode: {
    auto &comp = expr->Cast<SimplestVarConstComparison>();
    SerializeAttr(out, comp.attr.get());
    SerializeConstVar(out, comp.const_var.get());
    break;
  }
  case VarComparisonNode: {
    auto &comp = expr->Cast<SimplestVarComparison>();
    SerializeAttr(out, comp.left_attr.get());
    SerializeAttr(out, comp.right_attr.get());
    break;
  }
  case LogicalExprNode: {
    auto &logical = expr->Cast<SimplestLogicalExpr>();
    SimplestLogicalOp op = logical.GetLogicalOp();
    out.write(reinterpret_cast<const char *>(&op), sizeof(op));
    SerializeExpr(out, logical.left_expr.get());
    SerializeExpr(out, logical.right_expr.get());
    break;
  }
  case IsNullExprNode: {
    auto &is_null = expr->Cast<SimplestIsNullExpr>();
    SerializeAttr(out, is_null.attr.get());
    break;
  }
  case SingleAttrExprNode: {
    auto &single_attr = expr->Cast<SimplestSingleAttrExpr>();
    SerializeAttr(out, single_attr.attr.get());
    break;
  }
  default:
    throw std::runtime_error("Unknown SimplestExpr node type: " +
                             std::to_string(node_type));
  }
}

// Deserialize SimplestExpr
static std::unique_ptr<SimplestExpr> DeserializeExpr(std::istream &in) {
  SimplestNodeType node_type;
  in.read(reinterpret_cast<char *>(&node_type), sizeof(node_type));

  if (node_type == InvalidNodeType) {
    return nullptr;
  }

  SimplestExprType expr_type;
  in.read(reinterpret_cast<char *>(&expr_type), sizeof(expr_type));

  switch (node_type) {
  case VarConstComparisonNode: {
    auto attr = DeserializeAttr(in);
    auto const_var = DeserializeConstVar(in);
    return std::make_unique<SimplestVarConstComparison>(
        expr_type, std::move(attr), std::move(const_var));
  }
  case VarComparisonNode: {
    auto left_attr = DeserializeAttr(in);
    auto right_attr = DeserializeAttr(in);
    return std::make_unique<SimplestVarComparison>(
        expr_type, std::move(left_attr), std::move(right_attr));
  }
  case LogicalExprNode: {
    SimplestLogicalOp op;
    in.read(reinterpret_cast<char *>(&op), sizeof(op));
    auto left_expr = DeserializeExpr(in);
    auto right_expr = DeserializeExpr(in);
    return std::make_unique<SimplestLogicalExpr>(op, std::move(left_expr),
                                                 std::move(right_expr));
  }
  case IsNullExprNode: {
    auto attr = DeserializeAttr(in);
    return std::make_unique<SimplestIsNullExpr>(expr_type, std::move(attr));
  }
  case SingleAttrExprNode: {
    auto attr = DeserializeAttr(in);
    return std::make_unique<SimplestSingleAttrExpr>(std::move(attr));
  }
  default:
    throw std::runtime_error(
        "Unknown SimplestExpr node type during deserialization: " +
        std::to_string(node_type));
  }
}

// Serialize SimplestStmt
static void SerializeStmt(std::ostream &out, const SimplestStmt *stmt) {
  if (!stmt) {
    SimplestNodeType null_type = InvalidNodeType;
    out.write(reinterpret_cast<const char *>(&null_type), sizeof(null_type));
    return;
  }

  SimplestNodeType node_type = stmt->GetNodeType();
  out.write(reinterpret_cast<const char *>(&node_type), sizeof(node_type));

  // Serialize target_list
  size_t target_list_size = stmt->target_list.size();
  out.write(reinterpret_cast<const char *>(&target_list_size),
            sizeof(target_list_size));
  for (const auto &attr : stmt->target_list) {
    SerializeAttr(out, attr.get());
  }

  // Serialize qual_vec
  size_t qual_vec_size = stmt->qual_vec.size();
  out.write(reinterpret_cast<const char *>(&qual_vec_size),
            sizeof(qual_vec_size));
  for (const auto &expr : stmt->qual_vec) {
    SerializeExpr(out, expr.get());
  }

  // Serialize children
  size_t children_size = stmt->children.size();
  out.write(reinterpret_cast<const char *>(&children_size),
            sizeof(children_size));
  for (const auto &child : stmt->children) {
    SerializeStmt(out, child.get());
  }

  // Serialize node-specific data
  switch (node_type) {
  case ScanNode: {
    auto &scan = stmt->Cast<SimplestScan>();
    unsigned int table_idx = scan.GetTableIndex();
    out.write(reinterpret_cast<const char *>(&table_idx), sizeof(table_idx));
    WriteString(out, scan.GetTableName());
    uint64_t card = scan.GetEstimatedCardinality();
    out.write(reinterpret_cast<const char *>(&card), sizeof(card));
    break;
  }
  case JoinNode: {
    auto &join = stmt->Cast<SimplestJoin>();
    SimplestJoinType join_type = join.GetSimplestJoinType();
    out.write(reinterpret_cast<const char *>(&join_type), sizeof(join_type));

    size_t join_cond_size = join.join_conditions.size();
    out.write(reinterpret_cast<const char *>(&join_cond_size),
              sizeof(join_cond_size));
    for (const auto &cond : join.join_conditions) {
      SimplestExprType cond_type = cond->GetSimplestExprType();
      out.write(reinterpret_cast<const char *>(&cond_type), sizeof(cond_type));
      SerializeAttr(out, cond->left_attr.get());
      SerializeAttr(out, cond->right_attr.get());
    }
    break;
  }
  case FilterNode: {
    // Filter has no additional data beyond qual_vec
    break;
  }
  case SortNode: {
    auto &sort = stmt->Cast<SimplestSort>();
    auto order_vec = sort.GetOrderStructVec();
    size_t order_vec_size = order_vec.size();
    out.write(reinterpret_cast<const char *>(&order_vec_size),
              sizeof(order_vec_size));
    for (const auto &order : order_vec) {
      out.write(reinterpret_cast<const char *>(&order.sort_col_idx),
                sizeof(order.sort_col_idx));
      out.write(reinterpret_cast<const char *>(&order.order_type),
                sizeof(order.order_type));
      out.write(reinterpret_cast<const char *>(&order.nulls_first),
                sizeof(order.nulls_first));
    }
    break;
  }
  case ProjectionNode: {
    auto &proj = stmt->Cast<SimplestProjection>();
    unsigned int table_idx = proj.GetIndex();
    out.write(reinterpret_cast<const char *>(&table_idx), sizeof(table_idx));
    break;
  }
  case ChunkNode: {
    auto &chunk = stmt->Cast<SimplestChunk>();
    unsigned int table_idx = chunk.GetTableIndex();
    out.write(reinterpret_cast<const char *>(&table_idx), sizeof(table_idx));

    auto contents = chunk.GetContents();
    size_t contents_size = contents.size();
    out.write(reinterpret_cast<const char *>(&contents_size),
              sizeof(contents_size));
    for (const auto &str : contents) {
      WriteString(out, str);
    }

    uint64_t card = chunk.GetEstimatedCardinality();
    out.write(reinterpret_cast<const char *>(&card), sizeof(card));
    break;
  }
  case HashNode: {
    auto &hash = stmt->Cast<SimplestHash>();
    size_t hash_keys_size = hash.hash_keys.size();
    out.write(reinterpret_cast<const char *>(&hash_keys_size),
              sizeof(hash_keys_size));
    for (const auto &key : hash.hash_keys) {
      SerializeAttr(out, key.get());
    }
    break;
  }
  default:
    throw std::runtime_error("Unknown SimplestStmt node type: " +
                             std::to_string(node_type));
  }
}

// Deserialize SimplestStmt
static std::unique_ptr<SimplestStmt> DeserializeStmt(std::istream &in) {
  SimplestNodeType node_type;
  in.read(reinterpret_cast<char *>(&node_type), sizeof(node_type));

  if (node_type == InvalidNodeType) {
    return nullptr;
  }

  // Deserialize target_list
  size_t target_list_size;
  in.read(reinterpret_cast<char *>(&target_list_size),
          sizeof(target_list_size));
  std::vector<std::unique_ptr<SimplestAttr>> target_list;
  for (size_t i = 0; i < target_list_size; i++) {
    target_list.push_back(DeserializeAttr(in));
  }

  // Deserialize qual_vec
  size_t qual_vec_size;
  in.read(reinterpret_cast<char *>(&qual_vec_size), sizeof(qual_vec_size));
  std::vector<std::unique_ptr<SimplestExpr>> qual_vec;
  for (size_t i = 0; i < qual_vec_size; i++) {
    qual_vec.push_back(DeserializeExpr(in));
  }

  // Deserialize children
  size_t children_size;
  in.read(reinterpret_cast<char *>(&children_size), sizeof(children_size));
  std::vector<std::unique_ptr<SimplestStmt>> children;
  for (size_t i = 0; i < children_size; i++) {
    children.push_back(DeserializeStmt(in));
  }

  // Deserialize node-specific data and construct the appropriate node
  switch (node_type) {
  case ScanNode: {
    unsigned int table_idx;
    in.read(reinterpret_cast<char *>(&table_idx), sizeof(table_idx));
    std::string table_name = ReadString(in);
    uint64_t card;
    in.read(reinterpret_cast<char *>(&card), sizeof(card));

    // Create base_stmt with children
    auto base_stmt = std::make_unique<SimplestStmt>(std::move(children),
                                                    std::move(target_list),
                                                    SimplestNodeType::ScanNode);

    auto scan = std::make_unique<SimplestScan>(std::move(base_stmt), table_idx,
                                               table_name, card);
    scan->qual_vec = std::move(qual_vec);
    return scan;
  }
  case JoinNode: {
    SimplestJoinType join_type;
    in.read(reinterpret_cast<char *>(&join_type), sizeof(join_type));

    size_t join_cond_size;
    in.read(reinterpret_cast<char *>(&join_cond_size), sizeof(join_cond_size));
    std::vector<std::unique_ptr<SimplestVarComparison>> join_conditions;
    for (size_t i = 0; i < join_cond_size; i++) {
      SimplestExprType cond_type;
      in.read(reinterpret_cast<char *>(&cond_type), sizeof(cond_type));
      auto left_attr = DeserializeAttr(in);
      auto right_attr = DeserializeAttr(in);
      join_conditions.push_back(std::make_unique<SimplestVarComparison>(
          cond_type, std::move(left_attr), std::move(right_attr)));
    }

    // Create base_stmt with children
    auto base_stmt = std::make_unique<SimplestStmt>(std::move(children),
                                                    SimplestNodeType::JoinNode);

    // Create Join from base_stmt
    auto join = std::make_unique<SimplestJoin>(
        std::move(base_stmt), std::move(join_conditions), join_type);
    return join;
  }
  case FilterNode: {
    // Create base_stmt with children, target_list, qual_vec
    auto base_stmt = std::make_unique<SimplestStmt>(
        std::move(children), std::move(target_list), std::move(qual_vec),
        SimplestNodeType::FilterNode);

    auto filter = std::make_unique<SimplestFilter>(std::move(base_stmt));
    return filter;
  }
  case SortNode: {
    size_t order_vec_size;
    in.read(reinterpret_cast<char *>(&order_vec_size), sizeof(order_vec_size));
    std::vector<SimplestOrderStruct> order_vec;
    for (size_t i = 0; i < order_vec_size; i++) {
      SimplestOrderStruct order;
      in.read(reinterpret_cast<char *>(&order.sort_col_idx),
              sizeof(order.sort_col_idx));
      in.read(reinterpret_cast<char *>(&order.order_type),
              sizeof(order.order_type));
      in.read(reinterpret_cast<char *>(&order.nulls_first),
              sizeof(order.nulls_first));
      order_vec.push_back(order);
    }

    // Create base_stmt with children and target_list
    auto base_stmt = std::make_unique<SimplestStmt>(std::move(children),
                                                    std::move(target_list),
                                                    SimplestNodeType::SortNode);

    auto sort = std::make_unique<SimplestSort>(std::move(base_stmt),
                                               std::move(order_vec));
    return sort;
  }
  case ProjectionNode: {
    unsigned int table_idx;
    in.read(reinterpret_cast<char *>(&table_idx), sizeof(table_idx));

    // Create base_stmt with children and target_list
    auto base_stmt = std::make_unique<SimplestStmt>(
        std::move(children), std::move(target_list),
        SimplestNodeType::ProjectionNode);

    auto proj =
        std::make_unique<SimplestProjection>(std::move(base_stmt), table_idx);
    return proj;
  }
  case ChunkNode: {
    unsigned int table_idx;
    in.read(reinterpret_cast<char *>(&table_idx), sizeof(table_idx));

    size_t contents_size;
    in.read(reinterpret_cast<char *>(&contents_size), sizeof(contents_size));
    std::vector<std::string> contents;
    for (size_t i = 0; i < contents_size; i++) {
      contents.push_back(ReadString(in));
    }

    uint64_t card;
    in.read(reinterpret_cast<char *>(&card), sizeof(card));

    // Create base_stmt with children
    auto base_stmt = std::make_unique<SimplestStmt>(
        std::move(children), SimplestNodeType::ChunkNode);

    auto chunk = std::make_unique<SimplestChunk>(std::move(base_stmt),
                                                 table_idx, contents, card);
    return chunk;
  }
  case HashNode: {
    // HashNode needs hash_keys - serialize/deserialize them
    size_t hash_keys_size;
    in.read(reinterpret_cast<char *>(&hash_keys_size), sizeof(hash_keys_size));
    std::vector<std::unique_ptr<SimplestAttr>> hash_keys;
    for (size_t i = 0; i < hash_keys_size; i++) {
      hash_keys.push_back(DeserializeAttr(in));
    }

    auto hash = std::make_unique<SimplestHash>(std::move(children[0]),
                                               std::move(hash_keys));
    return hash;
  }
  default:
    throw std::runtime_error(
        "Unknown SimplestStmt node type during deserialization: " +
        std::to_string(node_type));
  }
}

// Public API: Save SimplestIR to file
void SaveSimplestIRToFile(const std::unique_ptr<SimplestStmt> &ir,
                          const std::string &filename) {
  std::ofstream file(filename, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open file for writing: " + filename);
  }

  // Write a magic number and version for future compatibility
  const uint32_t magic = 0x53495246; // "SIRF" (SimplestIR File)
  const uint32_t version = 1;
  file.write(reinterpret_cast<const char *>(&magic), sizeof(magic));
  file.write(reinterpret_cast<const char *>(&version), sizeof(version));

  // Serialize the IR tree
  SerializeStmt(file, ir.get());

  file.close();
}

// Public API: Load SimplestIR from file
std::unique_ptr<SimplestStmt>
LoadSimplestIRFromFile(const std::string &filename) {
  std::ifstream file(filename, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open file for reading: " + filename);
  }

  // Read and verify magic number and version
  uint32_t magic, version;
  file.read(reinterpret_cast<char *>(&magic), sizeof(magic));
  file.read(reinterpret_cast<char *>(&version), sizeof(version));

  if (magic != 0x53495246) {
    throw std::runtime_error(
        "Invalid SimplestIR file format (bad magic number)");
  }

  if (version != 1) {
    throw std::runtime_error("Unsupported SimplestIR file version: " +
                             std::to_string(version));
  }

  // Deserialize the IR tree
  auto ir = DeserializeStmt(file);

  file.close();
  return ir;
}

} // namespace ir_sql_converter
