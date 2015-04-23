#ifndef FSTDICT_FST_H
#define FSTDICT_FST_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <istream>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

size_t commonPrefixLen(const std::string &a, const std::string &b) {
  return std::mismatch(a.begin(), a.end(), b.begin()).first - a.begin();
}

// T must be an unsigned integer type
template<typename T>
void WriteUint(std::ostream *os, T value) {
  char buf;
  for (size_t i = 0; i < sizeof(T); ++i) {
    buf = static_cast<char>(value);
    os->write(&buf, 1);
    value /= 256;
  }
}

// T must be an unsigned integer type
template<typename T>
T ReadUint(std::istream *is) {
  T value = 0;
  char buf;
  for (size_t i = 0; i < sizeof(T); ++i) {
    value *= 256;
    is->read(&buf, 1);
    value |= static_cast<unsigned char>(buf);
  }
  return value;
}

}  // namespace

namespace FstDict {

using std::cerr;
using std::dec;
using std::endl;
using std::hex;
using std::ios;
using std::istream;
using std::make_pair;
using std::make_shared;
using std::mismatch;
using std::move;
using std::ostream;
using std::pair;
using std::reverse_copy;
using std::set;
using std::setfill;
using std::setw;
using std::shared_ptr;
using std::sort;
using std::string;
using std::stringstream;
using std::swap;
using std::unordered_map;
using std::uppercase;
using std::vector;

// Pair implements a pair of input and output.
struct Pair {
  string in;
  int32_t out;
  bool operator<(const Pair &that) const {
    return this->in < that.in;
  }
};

struct State {
  int id;
  unordered_map<uint8_t, shared_ptr<State>> trans;
  unordered_map<uint8_t, int32_t> output;
  set<int32_t> tail;
  bool isFinal;
  int64_t hcode;

  bool hasTail() {
    return !tail.empty();
  }

  void addTail(int32_t t) {
    tail.insert(t);
  }

  const set<int32_t> &getTails() {
    return tail;
  }

  void removeOutput(uint8_t ch) {
    constexpr int magic = 8191;
    auto it = output.find(ch);
    if (it != output.end() && it->second) {
      hcode -= ((int64_t)ch + it->second) * magic;
    }
    if (it != output.end()) {
      output.erase(it);
    }
  }

  void setOutput(uint8_t ch, int32_t out) {
    if (out == 0) {
      return;
    }
    output.insert(make_pair(ch, out));

    constexpr int magic = 8191;
    hcode += ((int64_t)ch + out) * magic;
  }

  void setTransition(uint8_t ch, shared_ptr<State> next) {
    trans.insert(make_pair(ch, next));

    constexpr int magic = 1001;
    hcode += ((int64_t)ch + next->id) * magic;
  }

  void renew() {
    trans.clear();
    output.clear();
    tail.clear();
    isFinal = false;
    hcode = 0;
  }

  bool operator==(const State &that) const {
    if (this == &that) {
      return true;
    }
    if (this->hcode != that.hcode) {
      return false;
    }
    if (this->trans.size() != that.trans.size() ||
        this->output.size() != that.output.size() ||
        this->tail.size() != that.tail.size() ||
        this->isFinal != that.isFinal) {
      return false;
    }
    if (this->trans != that.trans ||
        this->output != that.output ||
        this->tail != that.tail) {
      return false;
    }
    return true;
  }

  // toString returns a string representaion of a node for debug.
  string toString() {
    stringstream ss;
    ss << dec << id << "[" << hex << this << "]:";
    for (const auto &it : trans) {
      const auto &ch = it.first;
      const auto &tr = it.second;
      const auto &out = output.at(ch);
      ss << hex << uppercase << setw(2) << setfill('0') << ch
         << "/" << dec << out
         << "-->" << hex << setw(sizeof(tr)*2) << tr << ", ";
    }
    if (isFinal) {
      ss << " (tail:" << hex;
      for (const auto &t : tail) {
        ss << t << ", ";
      }
      ss << ")";
    }
    return ss.str();
  }
};

enum class Operation : uint8_t {
  Accept = 1,
  AcceptBreak = 2,
  Match = 3,
  Break = 4,
  Output = 5,
  OutputBreak = 6
};

string getOperationString(Operation op) {
  static const char* opStr[] = { "NA", "ACC", "ACB", "MTC", "BRK", "OUT", "OUB" };
  static size_t opStrLen = sizeof(opStr) / sizeof(opStr[0]);
  uint8_t opNum = static_cast<uint8_t>(op);
  if (opNum < opStrLen) {
    return opStr[opNum];
  }
  return string("NA");
}

union Instruction {
  struct {
    Operation op;
    uint8_t ch;
    uint16_t jump;
  } ops;
  int32_t v32;
};

// Configuration represents a FST (virtual machine) configuration.
struct Configuration {
  int pc;  // program counter
  int hd;  // input head
  vector<int32_t> out;  // outputs
  Configuration(int pc, int hd) : pc(pc), hd(hd) {};
};

// FST represents a finite state transducer (virtual machine).
struct FST {
  vector<Instruction> prog;
  vector<int32_t> data;

  // toString returns debug codes of a fst virtual machine.
  string toString() {
    stringstream ss;
    for (size_t pc = 0; pc < prog.size(); ++pc) {
      auto code = &prog[pc];
      auto &op = code->ops.op;
      auto &ch = code->ops.ch;
      auto &jump = code->ops.jump;
      switch (op) {
      case Operation::Accept:
      case Operation::AcceptBreak: {
        ss << setw(3) << pc << " " << getOperationString(op)
           << "\t" << dec << ch << " " << jump << endl;
        if (ch == 0) {
          break;
        }
        ++pc;
        code = &prog[pc];
        auto &to = code->v32;
        ss << setw(3) << pc << " [" << to << "]" << endl;
        ++pc;
        code = &prog[pc];
        auto &from = code->v32;
        ss << setw(3) << pc << " [" << from << "] ";
        for (int i = from; i < to; ++i) {
          ss << data[i] << ", ";  // FIXME
        }
        ss << endl;
        break;
      }
      case Operation::Match:
      case Operation::Break: {
        ss << setw(3) << pc << " " << getOperationString(op)
           << "\t" << hex << setfill('0') << setw(2) << ch
           << "(" << dec << ch << ") " << jump << endl;
        if (jump == 0) {
          ++pc;
          code = &prog[pc];
          auto& v32 = code->v32;
          ss << setw(3) << pc << " jmp[" << v32 << "]" << endl;
        }
        break;
      }
      case Operation::Output:
      case Operation::OutputBreak: {
        ss << setw(3) << pc << " " << getOperationString(op)
           << "\t" << hex << setfill('0') << setw(2) << ch
           << "(" << dec << ch << ") " << jump << endl;
        if (jump == 0) {
          ++pc;
          code = &prog[pc];
          ss << setw(3) << pc << " jmp[" << code->v32 << "]" << endl;
        }
        ++pc;
        code = &prog[pc];
        ss << setw(3) << pc << " [" << code->v32 << "]" << endl;
        break;
      }
      default: {
        ss << setw(3) << pc << " UNDEF " << code->v32 << endl;
      }
      }
    }
    return ss.str();
  }

  vector<Configuration> run(string input, bool *accept) {
    vector<Configuration> snap;
    int pc = 0;  // program counter
    int hd = 0;  // input head
    int32_t out;  // output
    *accept = false;
    Operation op;

    while (pc < prog.size() && hd <= input.size()) {
      auto code = &prog[pc];  // tmp instruction
      op = code->ops.op;  // operation
      auto &ch = code->ops.ch;  // char
      auto &jump = code->ops.jump;  // jump (16bit)
      switch (op) {
      case Operation::Match:
      case Operation::Break: {
        if (hd == input.size()) {
          goto L_END;
        }
        if (ch != input[hd]) {
          if (op == Operation::Break) {
            return snap;
          }
          if (jump == 0) {
            ++pc;
          }
          ++pc;
          continue;
        }
        if (jump > 0) {
          pc += jump;
        } else {
          ++pc;
          code = &prog[pc];
          pc += code->v32;
        }
        ++hd;
        continue;
      }
      case Operation::Output:
      case Operation::OutputBreak: {
        if (hd == input.size()) {
          goto L_END;
        }
        if (ch != input[hd]) {
          if (op == Operation::OutputBreak) {
            return snap;
          }
          if (jump == 0) {
            ++pc;
          }
          ++pc;
          ++pc;
          continue;
        }
        ++pc;
        code = &prog[pc];
        out = code->v32;
        if (jump > 0) {
          pc += jump;
        } else {
          ++pc;
          code = &prog[pc];
          pc += code->v32;
        }
        ++hd;
        continue;
      }
      case Operation::Accept:
      case Operation::AcceptBreak: {
        Configuration c(pc, hd);
        ++pc;
        if (ch == 0) {
          c.out.push_back(out);
        } else {
          code = &prog[pc];
          auto to = code->v32;
          ++pc;
          code = &prog[pc];
          auto from = code->v32;
          c.out.insert(c.out.end(), data.begin() + from, data.begin() + to);
          ++pc;
        }
        snap.push_back(move(c));
        if (op == Operation::AcceptBreak || hd == input.size()) {
          goto L_END;
        }
        continue;
      }
      default: {
        return snap;
      }
      }
    }      
  L_END:
    if (hd != input.size()) {
      return snap;
    }
    if (op != Operation::Accept && op != Operation::AcceptBreak) {
      return snap;
    }
    *accept = true;
    return snap;
  }

  // Search runs a finite state transducer for a given input and returns outputs if accepted otherwise nil.
  vector<int32_t> Search(string input) {
    bool accept;
    auto snap = run(input, &accept);
    if (!accept || snap.size() == 0) {
      return vector<int32_t>();
    }
    return move(snap.back().out);
  }

  // PrefixSearch returns the longest commom prefix keyword and it's length in given input
  // if detected otherwise -1, nil.
  vector<int32_t> PrefixSearch(string input, int *length) {
    bool dummy;
    auto snap = run(input, &dummy);
    if (snap.size() == 0) {
      *length = -1;
      return vector<int32_t>();
    }
    auto &c = snap.back();
    *length = c.hd;
    return move(c.out);
  }

  // CommonPrefixSearch finds keywords sharing common prefix in given input
  // and returns it's lengths and outputs. Returns nil, nil if there does not common prefix keywords.
  vector<vector<int32_t>> CommonPrefixSearch(string input, vector<int> *lens) {
    vector<vector<int32_t>> outputs;
    lens->clear();
    bool dummy;
    auto snap = run(input, &dummy);
    if (snap.size() == 0) {
      return outputs;
    }
    for (auto &c : snap) {
      lens->push_back(c.hd);
      outputs.push_back(move(c.out));
    }
    return outputs;
  }

  // Write saves a program of finite state transducer (virtual machine)
  bool Write(ostream *w) {
    ios::sync_with_stdio(false);
    size_t dataLen = data.size();
    WriteUint(w, dataLen);
    for (auto &v : data) {
      WriteUint(w, static_cast<uint32_t>(v));
    }
    size_t progLen = prog.size();
    WriteUint(w, progLen);
    for (size_t pc = 0; pc < prog.size(); ++pc) {
      auto code = &prog[pc];
      auto &op = code->ops.op;
      auto &ch = code->ops.ch;
      auto &jump = code->ops.jump;

      // write op and ch
      WriteUint(w, static_cast<uint8_t>(op));
      WriteUint(w, static_cast<uint8_t>(ch));

      switch (op) {
      case Operation::Accept:
      case Operation::AcceptBreak: {
        if (ch == 0) {
          break;
        }
        ++pc;
        code = &prog[pc];
        auto &to = code->v32;
        WriteUint(w, static_cast<uint32_t>(to));
        ++pc;
        code = &prog[pc];
        auto &from = code->v32;
        WriteUint(w, static_cast<uint32_t>(from));
        break;
      }
      case Operation::Match:
      case Operation::Break: {
        WriteUint(w, static_cast<uint16_t>(jump));
        if (jump != 0) {
          break;
        }
        ++pc;
        code = &prog[pc];
        WriteUint(w, static_cast<uint32_t>(code->v32));
        break;
      }
      case Operation::Output:
      case Operation::OutputBreak: {
        WriteUint(w, static_cast<uint16_t>(jump));
        ++pc;
        code = &prog[pc];
        WriteUint(w, static_cast<uint32_t>(code->v32));
        
        if (jump != 0) {
          break;
        }
        ++pc;
        code = &prog[pc];
        WriteUint(w, static_cast<uint32_t>(code->v32));
      }
      default: {
        cerr << "undefined operation error" << endl;
        return false;
      }
      }
    }
    return true;
  }

  // Read loads a program of finite state transducer (virtual machine)
  bool Read(istream *r) {
    ios::sync_with_stdio(false);
    size_t dataLen = ReadUint<size_t>(r);
    data.reserve(dataLen);
    for (size_t i = 0; i < dataLen; ++i) {
      data.push_back(ReadUint<uint32_t>(r));
    }
    size_t progLen = ReadUint<size_t>(r);
    prog.reserve(progLen);
    Instruction code;
    for (size_t pc = 0; pc < progLen; ++pc) {
      Operation op = static_cast<Operation>(ReadUint<uint8_t>(r));
      uint8_t ch = ReadUint<uint8_t>(r);

      switch (op) {
      case Operation::Accept:
      case Operation::AcceptBreak: {
        code.ops.op = op;
        code.ops.ch = ch;
        code.ops.jump = 0;
        prog.push_back(code);
        if (ch == 0) {
          break;
        }
        code.v32 = static_cast<int32_t>(ReadUint<uint32_t>(r));
        prog.push_back(code);
        code.v32 = static_cast<int32_t>(ReadUint<uint32_t>(r));
        prog.push_back(code);
        break;
      }
      case Operation::Match:
      case Operation::Break: {
        code.ops.op = op;
        code.ops.ch = ch;
        code.ops.jump = ReadUint<uint16_t>(r);
        prog.push_back(code);

        if (code.ops.jump != 0) {
          break;
        }
        code.v32 = static_cast<int32_t>(ReadUint<uint32_t>(r));
        prog.push_back(code);
        break;
      }
      case Operation::Output:
      case Operation::OutputBreak: {
        code.ops.op = op;
        code.ops.ch = ch;
        uint16_t jump = ReadUint<uint16_t>(r);
        code.ops.jump = jump;
        prog.push_back(code);
        code.v32 = static_cast<int32_t>(ReadUint<uint32_t>(r));
        prog.push_back(code);
        
        if (jump != 0) {
          break;
        }
        code.v32 = static_cast<int32_t>(ReadUint<uint32_t>(r));
        prog.push_back(code);
        break;
      }
      default: {
        cerr << "invalid format: undefined operation error" << endl;
        return false;
      }
      }
    }
    return true;
  }

};

// mast represents a Minimal Acyclic Subsequential Transeducer.
struct Mast {
  shared_ptr<State> initialState;
  vector<shared_ptr<State>> states;
  vector<shared_ptr<State>> finalStates;

  void addState(shared_ptr<State> n) {
    n->id = states.size();
    states.push_back(n);
    if (n->isFinal) {
      finalStates.push_back(n);
    }
  }

  vector<int32_t> run(const string &input, bool* ok) {
    auto s = initialState;
    vector<int32_t> out;
    for (size_t i = 0; i < input.length(); ++i) {
      auto o = s->output.find((uint8_t)input[i]);
      if (o != s->output.end()) {
        out.push_back(o->second);
      }
      auto it = s->trans.find((uint8_t)input[i]);
      if (it != s->trans.end()) {
        s = it->second;
      } else {
        *ok = false;
        return out;
      }
    }
    for (auto t : s->getTails()) {
      out.push_back(t);
    }
    return out;
  }

  bool accept(const string &input) {
    auto s = initialState;
    for (size_t i = 0; i < input.length(); ++i) {
      auto it = s->trans.find((uint8_t)input[i]);
      if (it != s->trans.end()) {
        s = it->second;
      } else {
        return false;
      }
    }
    return true;
  }

  void dot(ostream& w) {
    w << "digraph G {";
    w << "\trankdir=LR;";
    w << "\tnode [shape=circle]";
    for (const auto &s : finalStates) {
      w << "\t" << dec << s->id << "[peripheries = 2];" << endl;
    }
    for (const auto &from : states) {
      for (const auto &p : from->trans) {
        const auto &in = p.first;
        const auto &to = p.second;
        w << "\t" << from->id << " -> " << to->id;
        w << " [label=\""
          << setfill('0') << setw(2) << hex << in
          << dec << from->output.at(in);
        if (to->hasTail()) {
          for (const auto &t : to->getTails()) {
            w << t << ", ";
          }
        }
        w << "\"];";
      }
    }
    w << "}";
  }

  shared_ptr<FST> buildMachine(string *err) {
    vector<Instruction> prog;
    vector<int32_t> data;
    Instruction code;  // tmp instruction

    set<uint8_t> edges;
    unordered_map<int, int> addrMap;
    for (auto s : states) {
      edges.clear();
      for (auto p : s->trans) {
        edges.insert(p.first);
      }
      for (auto it = edges.crbegin(); it != edges.crend(); ++it) {
        auto ch = *it;
        auto next = s->trans.at(ch);
        auto out = s->output.at(ch);
        auto itAddr = addrMap.find(next->id);
        if (itAddr == addrMap.end() && !next->isFinal) {
          stringstream ss;
          ss << "next addr is undefined: state(" << dec << s->id
             << "), input(" << hex << ch << ")";
          *err = ss.str();
        }
        size_t jump = prog.size() - itAddr->second + 1;
        Operation op;
        if (out != 0) {
          if (it == edges.crbegin()) {
            op = Operation::OutputBreak;
          } else {
            op = Operation::Output;
          }
        } else if (it == edges.crbegin()) {
          op = Operation::Break;
        } else {
          op = Operation::Match;
        }
        
        if (jump > UINT16_MAX) {
          code.v32 = (int32_t)jump;
          prog.push_back(code);
          jump = 0;
        }
        if (out != 0) {
          code.v32 = (int32_t)out;
          prog.push_back(code);
        }

        code.ops.op = op;
        code.ops.ch = ch;
        code.ops.jump = (uint16_t)jump;
        prog.push_back(code);
      }
      if (s->isFinal) {
        if (!s->tail.empty()) {
          code.v32 = (int32_t)data.size();
          prog.push_back(code);
          for (auto t : s->tail) {
            data.push_back(t);
          }
          code.v32 = (int32_t)data.size();
          prog.push_back(code);
        }
        if (s->trans.empty()) {
          code.ops.op = Operation::Accept;
        } else {
          code.ops.op = Operation::AcceptBreak;
        }
        // clear
        code.ops.ch = 0;
        code.ops.jump = 0;
        if (!s->tail.empty()) {
          code.ops.ch = 1;
        }
        prog.push_back(code);
      }
      addrMap.insert(make_pair(s->id, (int)prog.size()));
    }
    auto t = make_shared<FST>();
    reverse_copy(prog.begin(), prog.end(), t->prog.begin());
    t->data = move(data);
    return t;
  }
};

shared_ptr<Mast> buildMAST(vector<Pair> *input) {
  auto m = make_shared<Mast>();

  sort(input->begin(), input->end());

  constexpr size_t initialMASTSize = 1024;
  unordered_map<int64_t, vector<shared_ptr<State>>> dict;
  m->states.clear();
  m->states.reserve(initialMASTSize);
  m->finalStates.clear();
  m->finalStates.reserve(initialMASTSize);

  size_t maxInputWordLen = 0;
  for (const auto &pair : *input) {
    if (pair.in.size() > maxInputWordLen) {
      maxInputWordLen = pair.in.size();
    }
  }

  vector<shared_ptr<State>> buf(maxInputWordLen+1);
  for (size_t i = 0; i < buf.size(); ++i) {
    buf[i] = make_shared<State>();
  }

  string prev;
  for (const auto &pair : *input) {
    auto in = pair.in;
    auto out = pair.out;
    bool fZero = (out == 0);  // flag
    size_t prefixLen = commonPrefixLen(in, prev);
    for (size_t i = prev.length(); i > prefixLen; --i) {
      shared_ptr<State> s;
      const auto it = dict.find(buf[i]->hcode);
      if (it != dict.end()) {
        const auto &cs = it->second;
        for (auto &c : cs) {
          if (*c == *buf[i]) {
            s = c;
            break;
          }
        }
      } else {
        dict.insert(make_pair(buf[i]->hcode, vector<shared_ptr<State>>()));
      }
      if (!s) {
        s = make_shared<State>(*buf[i]);
        m->addState(s);
        dict.at(buf[i]->hcode).push_back(s);
      }
      buf[i]->renew();
      buf[i-1]->setTransition((uint8_t)prev[i-1], s);
    }
    for (size_t i = prefixLen+1; i < in.length(); ++i) {
      buf[i-1]->setTransition((uint8_t)in[i-1], buf[i]);
    }
    if (in != prev) {
      buf[in.length()]->isFinal = true;
    }
    for (size_t j = 1; j < prefixLen+1; ++j) {
      if (buf[j-1]->output.at((uint8_t)in[j-1]) == out) {
        out = 0;
        break;
      }
      int32_t outSuff = buf[j-1]->output.at((uint8_t)in[j-1]);
      buf[j-1]->removeOutput((uint8_t)in[j-1]);  // clear the prev edge
      for (const auto &elem : buf[j]->trans) {
        const auto &ch = elem.first;
        buf[j]->setOutput(ch, outSuff);
      }
      if (buf[j]->isFinal && outSuff != 0) {
        buf[j]->addTail(outSuff);
      }
    }
    if (in != prev) {
      buf[prefixLen]->setOutput((uint8_t)in[prefixLen], out);
    } else if (fZero || out != 0) {
      buf[in.length()]->addTail(out);
    }
    prev = in;
  }
  // flush the buf
  for (size_t i = prev.length(); i > 0; --i) {
    shared_ptr<State> s;
    const auto it = dict.find(buf[i]->hcode);
    if (it != dict.end()) {
      const auto &cs = it->second;
      for (auto &c : cs) {
        if (*c == *buf[i]) {
          s = c;
          break;
        }
      }
    } else {
      dict.insert(make_pair(buf[i]->hcode, vector<shared_ptr<State>>()));
    }
    if (!s) {
      s = make_shared<State>(*buf[i]);
      buf[i]->renew();
      m->addState(s);
      dict.at(buf[i]->hcode).push_back(s);
    }
    buf[i-1]->setTransition((uint8_t)prev[i-1], s);
  }
  m->initialState = buf[0];
  m->addState(buf[0]);
  return m;
}

// BuildFST constructs a virtual machine of a finite state transducer from a given inputs.
shared_ptr<FST> BuildFST(vector<Pair> *input, string *err) {
  auto m = buildMAST(input);
  auto ret = m->buildMachine(err);
  return ret;
}

}  // namespace FstDict
#endif  // FSTDICT_FST_H
