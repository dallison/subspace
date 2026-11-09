// Copyright 2026 David Allison
// Copyright 2026 General Motors
// All Rights Reserved
// See LICENSE file for licensing information.

#pragma once

#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include <cctype>
#include <string_view>

namespace subspace {

// Reports whether the message, or any message inside it, has a field this
// build doesn't know. The text format rejects those, so the wire format does
// too, rather than dropping them.
inline bool HasUnknownFields(const google::protobuf::Message &message) {
  const google::protobuf::Reflection *reflection = message.GetReflection();
  if (!reflection->GetUnknownFields(message).empty()) {
    return true;
  }
  const google::protobuf::Descriptor *descriptor = message.GetDescriptor();
  for (int i = 0; i < descriptor->field_count(); i++) {
    const google::protobuf::FieldDescriptor *field = descriptor->field(i);
    if (field->cpp_type() !=
        google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
      continue;
    }
    if (field->is_repeated()) {
      const int count = reflection->FieldSize(message, field);
      for (int j = 0; j < count; j++) {
        if (HasUnknownFields(
                reflection->GetRepeatedMessage(message, field, j))) {
          return true;
        }
      }
    } else if (reflection->HasField(message, field) &&
               HasUnknownFields(reflection->GetMessage(message, field))) {
      return true;
    }
  }
  return false;
}

// A configuration file is protobuf text unless its extension is .pb or
// .binpb, which is the wire format. The comparison ignores case. A name
// with any other extension, or none, is text.
inline bool ConfigFileIsText(std::string_view path) {
  const size_t slash = path.find_last_of('/');
  const std::string_view base =
      slash == std::string_view::npos ? path : path.substr(slash + 1);
  const size_t dot = base.find_last_of('.');
  if (dot == std::string_view::npos || dot + 1 >= base.size()) {
    return true;
  }
  const std::string_view ext = base.substr(dot + 1);
  auto same = [](std::string_view a, std::string_view b) {
    if (a.size() != b.size()) {
      return false;
    }
    for (size_t i = 0; i < a.size(); i++) {
      const unsigned char ca = static_cast<unsigned char>(a[i]);
      const unsigned char cb = static_cast<unsigned char>(b[i]);
      if (std::tolower(ca) != std::tolower(cb)) {
        return false;
      }
    }
    return true;
  };
  return !same(ext, "pb") && !same(ext, "binpb");
}

} // namespace subspace
