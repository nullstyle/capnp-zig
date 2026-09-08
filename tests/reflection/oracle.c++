#include <capnp/dynamic.h>
#include <capnp/message.h>
#include <capnp/schema-loader.h>
#include <capnp/schema.capnp.h>
#include <capnp/serialize.h>
#include <kj/debug.h>
#include <kj/exception.h>

#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <string>

// KJ 1.x and 2.x spell Maybe access differently. Let the linked KJ library
// catch its exceptions so the oracle also works with system shared libraries.
template <typename Callback>
bool catches(Callback&& callback, std::string& description) {
  auto result = kj::runCatchingExceptions(kj::fwd<Callback>(callback));
#ifdef KJ_IF_SOME
  KJ_IF_SOME(error, result) { description = error.getDescription().cStr(); return true; }
#else
  KJ_IF_MAYBE(error, result) { description = error->getDescription().cStr(); return true; }
#endif
  return false;
}

kj::Array<capnp::word> readWords(const char* path) {
  std::ifstream input(path, std::ios::binary | std::ios::ate);
  KJ_REQUIRE(input.good(), "cannot open input", path);
  auto size = input.tellg();
  KJ_REQUIRE(size >= 0 && size % sizeof(capnp::word) == 0, "unaligned message", path);
  auto words = kj::heapArray<capnp::word>(static_cast<size_t>(size) / sizeof(capnp::word));
  input.seekg(0);
  input.read(reinterpret_cast<char*>(words.begin()), size);
  KJ_REQUIRE(input.good(), "cannot read input", path);
  return words;
}

uint64_t findType(capnp::schema::CodeGeneratorRequest::Reader request, const char* suffix) {
  for (auto node : request.getNodes()) {
    auto name = node.getDisplayName();
    if (std::string(name.cStr(), name.size()).ends_with(suffix)) return node.getId();
  }
  KJ_FAIL_REQUIRE("missing schema type", suffix);
}

void checkDescriptors(capnp::schema::CodeGeneratorRequest::Reader original,
                      capnp::schema::CodeGeneratorRequest::Reader embedded,
                      capnp::SchemaLoader& loader) {
  std::map<uint64_t, capnp::schema::Node::Reader> nodes;
  for (auto node : original.getNodes()) nodes.emplace(node.getId(), node);
  KJ_REQUIRE(nodes.size() == embedded.getNodes().size(), "descriptor node count changed");
  bool sawGroup = false, sawConst = false, sawAnnotation = false, sawInterface = false;
  for (auto node : embedded.getNodes()) {
    auto found = nodes.find(node.getId());
    KJ_REQUIRE(found != nodes.end(), "unexpected or duplicated embedded node", node.getId());
    // Keep the owning array alive while comparing both full pointer graphs.
    auto expectedWords = capnp::canonicalize(found->second);
    auto actualWords = capnp::canonicalize(node);
    KJ_REQUIRE(expectedWords.asBytes().size() == actualWords.asBytes().size() &&
      std::memcmp(expectedWords.begin(), actualWords.begin(), expectedWords.asBytes().size()) == 0,
      "embedded schema Node changed", node.getDisplayName());
    nodes.erase(found);
    loader.load(node);
    sawGroup |= node.isStruct() && node.getStruct().getIsGroup();
    sawConst |= node.isConst();
    sawAnnotation |= node.isAnnotation();
    sawInterface |= node.isInterface();
  }
  KJ_REQUIRE(nodes.empty() && sawGroup && sawConst && sawAnnotation && sawInterface);

  auto values = loader.get(findType(embedded, ":Values")).asStruct();
  KJ_REQUIRE(values.getProto().getAnnotations().size() == 1);
  auto tagged = values.getFieldByName("tagged").getProto();
  KJ_REQUIRE(tagged.getAnnotations()[0].getValue().getText() == "field annotation");
  auto details = values.getFieldByName("details").getType().asStruct();
  KJ_REQUIRE(details.getProto().getStruct().getIsGroup());
  auto mode = details.getFieldByName("mode").getType().asEnum();
  KJ_REQUIRE(mode.getEnumerantByName("archived").getOrdinal() == 1);

  auto brands = loader.get(findType(embedded, ":Brands")).asStruct();
  auto textBox = brands.getFieldByName("text").getType().asStruct();
  KJ_REQUIRE(textBox.getFieldByName("value").getType().isText());
  auto pair = brands.getFieldByName("pair").getType().asStruct();
  KJ_REQUIRE(pair.getFieldByName("first").getType().isText());
  KJ_REQUIRE(pair.getFieldByName("second").getType().isData());
  auto nested = brands.getFieldByName("nested").getType().asStruct()
    .getFieldByName("value").getType().asStruct()
    .getFieldByName("value").getType().asStruct();
  KJ_REQUIRE(nested.getProto().getId() == findType(embedded, ":Record"));
  auto unbound = brands.getFieldByName("unbound").getType().asStruct();
  KJ_REQUIRE(unbound.getFieldByName("value").getType().isAnyPointer());

  auto lookup = loader.get(findType(embedded, ":Lookup")).asInterface();
  auto method = lookup.getMethodByName("find");
  KJ_REQUIRE(method.getParamType().getFieldByName("name").getType().isText());
  KJ_REQUIRE(method.getResultType().getFieldByName("value").getType().asStruct()
    .getProto().getId() == findType(embedded, ":Scalars"));
}

void checkValues(capnp::DynamicStruct::Reader root) {
  KJ_REQUIRE(root.get("high").as<uint64_t>() == 42);
  KJ_REQUIRE(root.get("low").as<int64_t>() == INT64_MIN);
  KJ_REQUIRE(root.get("text").as<capnp::Text>() == "dynamic café");
  auto raw = root.get("raw").as<capnp::Data>();
  KJ_REQUIRE(raw.size() == 256);
  for (unsigned int i = 0; i < raw.size(); ++i) KJ_REQUIRE(raw[i] == i);
  auto data = root.get("inlineData").as<capnp::Data>();
  KJ_REQUIRE(data.size() == 3 && data[0] == 0 && data[1] == 128 && data[2] == 255);
  auto numbers = root.get("numbers").as<capnp::DynamicList>();
  KJ_REQUIRE(numbers.size() == 3 && numbers[0].as<uint64_t>() == 7 &&
    numbers[1].as<uint64_t>() == (uint64_t(1) << 63) && numbers[2].as<uint64_t>() == UINT64_MAX);
  auto record = root.get("record").as<capnp::DynamicStruct>();
  KJ_REQUIRE(record.get("label").as<capnp::Text>() == "mutable default");
  KJ_REQUIRE(record.get("mode").as<capnp::DynamicEnum>().getRaw() == 0);
  auto records = root.get("records").as<capnp::DynamicList>();
  KJ_REQUIRE(records.size() == 2 && records[1].as<capnp::DynamicStruct>()
    .get("label").as<capnp::Text>() == "list second");
  auto details = root.get("details").as<capnp::DynamicStruct>();
  KJ_REQUIRE(!details.get("enabled").as<bool>());
  KJ_REQUIRE(details.get("mode").as<capnp::DynamicEnum>().getRaw() == 60000);
  auto selected = root.get("selected").as<capnp::DynamicStruct>();
  KJ_REQUIRE(selected.get("name").as<capnp::Text>() == "dynamic union");
  auto payload = selected.get("payload").as<capnp::Data>();
  KJ_REQUIRE(payload.size() == 3 && payload[0] == 0 && payload[1] == 128 && payload[2] == 255);
}

void checkScalars(capnp::DynamicStruct::Reader root) {
  KJ_REQUIRE(!root.get("boolean").as<bool>());
  KJ_REQUIRE(root.get("signed8").as<int8_t>() == -128);
  KJ_REQUIRE(root.get("signed16").as<int16_t>() == -32768);
  KJ_REQUIRE(root.get("signed32").as<int32_t>() == INT32_MIN);
  KJ_REQUIRE(root.get("signed64").as<int64_t>() == INT64_MIN);
  KJ_REQUIRE(root.get("unsigned8").as<uint8_t>() == UINT8_MAX);
  KJ_REQUIRE(root.get("unsigned16").as<uint16_t>() == UINT16_MAX);
  KJ_REQUIRE(root.get("unsigned32").as<uint32_t>() == UINT32_MAX);
  KJ_REQUIRE(root.get("unsigned64").as<uint64_t>() == UINT64_MAX);
  KJ_REQUIRE(root.get("real32").as<float>() == -3.5f);
  KJ_REQUIRE(root.get("real64").as<double>() == 9.25);
  auto bits = root.get("booleans").as<capnp::DynamicList>();
  KJ_REQUIRE(bits.size() == 9 && bits[0].as<bool>() && !bits[1].as<bool>() && bits[8].as<bool>());
  auto signedValues = root.get("signed").as<capnp::DynamicList>();
  KJ_REQUIRE(signedValues.size() == 2 && signedValues[0].as<int16_t>() == -32768);
  auto texts = root.get("texts").as<capnp::DynamicList>();
  KJ_REQUIRE(texts.size() == 2 && texts[1].as<capnp::Text>() == "second 🦀");
  auto blobs = root.get("blobs").as<capnp::DynamicList>();
  KJ_REQUIRE(blobs.size() == 1 && blobs[0].as<capnp::Data>().size() == 3);
  auto nested = root.get("nested").as<capnp::DynamicList>();
  KJ_REQUIRE(nested.size() == 1 && nested[0].as<capnp::DynamicList>()[1].as<uint32_t>() == UINT32_MAX);
  auto choices = root.get("choices").as<capnp::DynamicList>();
  KJ_REQUIRE(choices.size() == 2 && choices[1].as<capnp::DynamicEnum>().getRaw() == 60000);
}

uint64_t readDataWord(capnp::AnyStruct::Reader value, unsigned index) {
  auto bytes = value.getDataSection();
  uint64_t result = 0;
  for (unsigned byte = 0; byte < 8 && index * 8 + byte < bytes.size(); ++byte)
    result |= uint64_t(bytes[index * 8 + byte]) << (8 * byte);
  return result;
}

void checkEvolution(const std::string& directory, capnp::StructSchema schema) {
  enum Kind { COMPOSITE, BYTE, U16, U32, U64, POINTER, VOID };
  enum Operation { GET, INIT, COPY };
  struct Case { const char* name; Kind kind = COMPOSITE; unsigned data = 1, pointers = 1, count = 3; Operation operation = GET; };
  Case cases[] = {
    {"inline-small"}, {"far-inline"}, {"inline-init", COMPOSITE, 1, 1, 3, INIT},
    {"inline-extra-data", COMPOSITE, 3, 1}, {"inline-extra-pointers", COMPOSITE, 1, 3},
    {"inline-zero-width", COMPOSITE, 0, 0}, {"byte", BYTE}, {"u16", U16},
    {"u32", U32}, {"u64", U64}, {"pointer", POINTER}, {"void", VOID},
    {"empty-inline", COMPOSITE, 1, 1, 0}, {"empty-byte", BYTE, 1, 1, 0},
    {"copy-large", COMPOSITE, 1, 1, 3, COPY},
  };
  const char* labels[] = {"first", "middle", "last"};
  const char* unknownLabels[] = {"unknown first", "unknown middle", "unknown last"};
  for (auto test : cases) {
    auto words = readWords((directory + "/evolution-" + test.name + ".bin").c_str());
    capnp::FlatArrayMessageReader reader(words);
    auto root = reader.getRoot<capnp::DynamicStruct>(schema);
    KJ_REQUIRE(root.get("marker").as<capnp::Text>() == "root survives", test.name);
    if (test.kind == BYTE && test.count > 0) {
      KJ_REQUIRE(root.get("single").as<capnp::DynamicStruct>().get("value").as<uint64_t>() == 0x12);
    }
    auto list = root.get("records").as<capnp::DynamicList>();
    KJ_REQUIRE(list.size() == test.count, test.name);
    if (test.count == 0) continue;
    auto rawList = reader.getRoot<capnp::AnyStruct>().getPointerSection()[0]
      .getAs<capnp::AnyList>().as<capnp::List<capnp::AnyStruct>>();
    for (unsigned i = 0; i < test.count; ++i) {
      uint64_t original = 0;
      switch (test.kind) {
        case COMPOSITE: original = test.data > 0 ? (i + 1) * 10 : 0; break;
        case BYTE: { uint8_t values[] = {0x12, 0x80, 0xff}; original = values[i]; break; }
        case U16: { uint16_t values[] = {0x1234, 0x8000, 0xffff}; original = values[i]; break; }
        case U32: { uint32_t values[] = {0x12345678, 0x80000000, 0xffffffff}; original = values[i]; break; }
        case U64: { uint64_t values[] = {0x123456789abcdef0, 0x8000000000000000, UINT64_MAX}; original = values[i]; break; }
        default: break;
      }
      bool copied = i == 1 && test.operation == COPY;
      bool initialized = i == 1 && test.operation == INIT;
      uint64_t value = copied ? 91 : initialized ? 77 : original;
      uint64_t extra = copied ? 92 : initialized ? 88 : i == 1 ? 222 :
        test.kind == COMPOSITE && test.data > 1 ? 100 + i : 0;
      const char* label = copied ? "copied" : initialized ? "reset" :
        test.kind == POINTER || (test.kind == COMPOSITE && test.pointers > 0) ? labels[i] : "";
      const char* note = copied ? "copied note" : initialized ? "initialized" : i == 1 ? "grown" :
        test.kind == COMPOSITE && test.pointers > 1 ? "prior note" : "";
      auto child = list[i].as<capnp::DynamicStruct>();
      KJ_REQUIRE(child.get("value").as<uint64_t>() == value && child.get("extra").as<uint64_t>() == extra, test.name, i);
      KJ_REQUIRE(child.get("label").as<capnp::Text>() == label && child.get("note").as<capnp::Text>() == note, test.name, i);
      auto raw = rawList[i];
      uint64_t unknown = copied ? 0xa55a : initialized ? 0 : test.kind == COMPOSITE && test.data > 2 ? 0xfeed0000 + i : 0;
      KJ_REQUIRE(readDataWord(raw, 2) == unknown, test.name, i);
      const char* unknownText = copied ? "copied unknown" : initialized ? "" :
        test.kind == COMPOSITE && test.pointers > 2 ? unknownLabels[i] : "";
      if (raw.getPointerSection().size() > 2) {
        KJ_REQUIRE(raw.getPointerSection()[2].getAs<capnp::Text>() == unknownText, test.name, i);
      } else { KJ_REQUIRE(unknownText[0] == '\0', test.name, i); }
    }
  }
  for (const char* name : {"boolean-rejected", "empty-boolean-rejected"}) {
    auto bitWords = readWords((directory + "/evolution-" + name + ".bin").c_str());
    capnp::FlatArrayMessageReader bitReader(bitWords);
    std::string description;
    bool rejected = catches([&] {
      (void)bitReader.getRoot<capnp::DynamicStruct>(schema).get("records").as<capnp::DynamicList>().size();
    }, description);
    KJ_REQUIRE(description.find("upgrading boolean lists to structs") != std::string::npos,
      "unexpected boolean-list rejection", description);
    KJ_REQUIRE(rejected, "C++ unexpectedly accepted a boolean-to-struct list upgrade", name);
  }
  auto nestedWords = readWords((directory + "/evolution-nested.bin").c_str());
  capnp::FlatArrayMessageReader nestedReader(nestedWords);
  auto outer = nestedReader.getRoot<capnp::DynamicStruct>(schema).get("nested").as<capnp::DynamicList>();
  KJ_REQUIRE(outer.size() == 1);
  auto inner = outer[0].as<capnp::DynamicList>();
  KJ_REQUIRE(inner.size() == 3);
  uint64_t expected[] = {12, 34, 56};
  for (unsigned i = 0; i < 3; ++i)
    KJ_REQUIRE(inner[i].as<capnp::DynamicStruct>().get("value").as<uint64_t>() == expected[i]);
  auto middle = inner[1].as<capnp::DynamicStruct>();
  KJ_REQUIRE(middle.get("extra").as<uint64_t>() == 222 && middle.get("note").as<capnp::Text>() == "nested growth");
}

int main(int argc, char** argv) {
  std::string description;
  if (catches([&] {
    KJ_REQUIRE(argc == 6);
    auto originalWords = readWords(argv[1]);
    auto descriptorWords = readWords(argv[2]);
    auto valueWords = readWords(argv[3]);
    auto scalarWords = readWords(argv[4]);
    capnp::FlatArrayMessageReader originalReader(originalWords);
    capnp::FlatArrayMessageReader descriptorReader(descriptorWords);
    capnp::FlatArrayMessageReader valueReader(valueWords);
    capnp::FlatArrayMessageReader scalarReader(scalarWords);
    auto original = originalReader.getRoot<capnp::schema::CodeGeneratorRequest>();
    auto embedded = descriptorReader.getRoot<capnp::schema::CodeGeneratorRequest>();
    capnp::SchemaLoader loader;
    checkDescriptors(original, embedded, loader);
    checkValues(valueReader.getRoot<capnp::DynamicStruct>(loader.get(findType(embedded, ":Values")).asStruct()));
    checkScalars(scalarReader.getRoot<capnp::DynamicStruct>(loader.get(findType(embedded, ":Scalars")).asStruct()));
    checkEvolution(argv[5], loader.get(findType(embedded, ":Evolution")).asStruct());
  }, description)) {
    std::cerr << description << '\n';
    return 1;
  }
  return 0;
}
