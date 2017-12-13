#include <iostream>

#include "mruby.h"
#include "mruby/array.h"
#include "mruby/data.h"
#include "mruby/hash.h"
#include "mruby/proc.h"
#include "mruby/string.h"
#include "mruby/variable.h"

#include "rocksdb/db.h"

#define CLASSNAME "RocksDB"
#define ROCKSDB_HANDLER_EXCEPTION "RocksDBHandlerError"
#define E_ROCKSDB_ERROR (mrb_class_get_under(mrb, mrb_class_get(mrb, CLASSNAME), ROCKSDB_HANDLER_EXCEPTION))

const int PAIR_ARGC = 2;

using namespace std;

static inline rocksdb::DB*
_rocksdb_get_handler(mrb_state* mrb, mrb_value self)
{
  auto handler = reinterpret_cast<rocksdb::DB*>DATA_PTR(self);

  if (handler == nullptr) {
    mrb_raise(mrb, E_ROCKSDB_ERROR, "rocksdb is closed");
  }

  return handler;
}

static void
_rocksdb_close(mrb_state *mrb, void *p)
{
  delete reinterpret_cast<rocksdb::DB*>(p);
}

static const
struct mrb_data_type RocksDB_type = {
  CLASSNAME, _rocksdb_close,
};


/*
 * Core methods
 */
static mrb_value
mrb_rocksdb_open(mrb_state *mrb, mrb_value self)
{
  char* path;
  rocksdb::DB* handler;
  rocksdb::Options options;

  mrb_get_args(mrb, "z", &path);

  options.create_if_missing = true;

  auto status = rocksdb::DB::Open(options, path, &handler);

  if (!status.ok()) {
    mrb_raise(mrb, E_ROCKSDB_ERROR, "rocksdb open failed");
  }

  DATA_TYPE(self) = &RocksDB_type;
  DATA_PTR(self) = (void*)handler;

  return self;
}

static mrb_value
mrb_rocksdb_get(mrb_state *mrb, mrb_value self)
{
  char* key;
  mrb_get_args(mrb, "z", &key);

  string value;
  auto handler = _rocksdb_get_handler(mrb, self);
  const auto status = handler->Get(rocksdb::ReadOptions(), key, &value);

  return status.ok() ? mrb_str_new(mrb, value.c_str(), value.length()) : mrb_nil_value();
}

static mrb_value
mrb_rocksdb_set(mrb_state *mrb, mrb_value self)
{
  char *key, *value;
  mrb_get_args(mrb, "zz", &key, &value);

  auto handler = _rocksdb_get_handler(mrb, self);

  const auto status = handler->Put(rocksdb::WriteOptions(), key, value);
  if (!status.ok()) {
    mrb_raise(mrb, E_ROCKSDB_ERROR, "rocksdb set is failed");
  }

  return mrb_nil_value();
}

static mrb_value
mrb_rocksdb_delete(mrb_state *mrb, mrb_value self)
{
  char *key;
  mrb_get_args(mrb, "z", &key);

  auto handler = _rocksdb_get_handler(mrb, self);

  const auto status = handler->Delete(rocksdb::WriteOptions(), key);
  if (!status.ok()) {
    mrb_raise(mrb, E_ROCKSDB_ERROR, "rocksdb delete is failed");
  }

  return mrb_nil_value();
}

static mrb_value
mrb_rocksdb_each(mrb_state *mrb, mrb_value self)
{
  mrb_value block;
  mrb_get_args(mrb, "&", &block);

  auto handler = _rocksdb_get_handler(mrb, self);
  auto iter = handler->NewIterator(rocksdb::ReadOptions());
  iter->SeekToFirst();

  for (; iter->Valid(); iter->Next()) {
    auto k = iter->key();
    auto v = iter->value();

    mrb_value key = mrb_str_new(mrb, k.data(), k.size());
    mrb_value val = mrb_str_new(mrb, v.data(), v.size());
    mrb_value argv[PAIR_ARGC] = {key, val};
    mrb_yield_argv(mrb, block, PAIR_ARGC, argv);
  }

  return self;
}

static mrb_value
mrb_rocksdb_each_key(mrb_state *mrb, mrb_value self)
{
  mrb_value block;
  mrb_get_args(mrb, "&", &block);

  auto handler = _rocksdb_get_handler(mrb, self);
  auto iter = handler->NewIterator(rocksdb::ReadOptions());
  iter->SeekToFirst();

  for (; iter->Valid(); iter->Next()) {
    auto k = iter->key();

    mrb_value key = mrb_str_new(mrb, k.data(), k.size());
    mrb_yield(mrb, block, key);
  }

  return self;
}

static mrb_value
mrb_rocksdb_each_value(mrb_state *mrb, mrb_value self)
{
  mrb_value block;
  mrb_get_args(mrb, "&", &block);

  auto handler = _rocksdb_get_handler(mrb, self);
  auto iter = handler->NewIterator(rocksdb::ReadOptions());
  iter->SeekToFirst();

  for (; iter->Valid(); iter->Next()) {
    auto v = iter->value();

    mrb_value value = mrb_str_new(mrb, v.data(), v.size());
    mrb_yield(mrb, block, value);
  }

  return self;
}

static mrb_value
mrb_rocksdb_has_key_q(mrb_state *mrb, mrb_value self)
{
  auto has_key = !mrb_nil_p(mrb_rocksdb_get(mrb, self));
  return has_key ? mrb_true_value() : mrb_false_value();
}

static mrb_value
mrb_rocksdb_close(mrb_state *mrb, mrb_value self)
{
  delete _rocksdb_get_handler(mrb, self);
  DATA_PTR(self) = nullptr;

  return mrb_nil_value();
}

static mrb_value
mrb_rocksdb_closed_q(mrb_state *mrb, mrb_value self)
{
  auto handler = reinterpret_cast<rocksdb::DB*>(DATA_PTR(self));
  return handler == nullptr ? mrb_true_value() : mrb_false_value();
}


static mrb_value
mrb_rocksdb_invert(mrb_state *mrb, mrb_value self)
{
  auto handler = _rocksdb_get_handler(mrb, self);
  auto iter = handler->NewIterator(rocksdb::ReadOptions());
  iter->SeekToFirst();

  mrb_value hash = mrb_hash_new(mrb);

  for (; iter->Valid(); iter->Next()) {
    auto k = iter->key();
    auto v = iter->value();

    mrb_value key = mrb_str_new(mrb, k.data(), k.size());
    mrb_value val = mrb_str_new(mrb, v.data(), v.size());
    mrb_hash_set(mrb, hash, val, key);
  }

  return hash;
}

static mrb_value
mrb_rocksdb_shift(mrb_state *mrb, mrb_value self)
{
  auto handler = _rocksdb_get_handler(mrb, self);
  auto iter = handler->NewIterator(rocksdb::ReadOptions());
  iter->SeekToFirst();

  // return nil if empty
  if (!iter->Valid()) {
    return mrb_nil_value();
  }

  const auto k = iter->key();
  const auto v = iter->value();
  auto key = mrb_str_new(mrb, k.data(), k.size());
  auto val = mrb_str_new(mrb, v.data(), v.size());

  auto array = mrb_ary_new(mrb);
  mrb_ary_push(mrb, array, key);
  mrb_ary_push(mrb, array, val);

  const auto status = handler->Delete(rocksdb::WriteOptions(), k);
  if (!status.ok()) {
    mrb_raise(mrb, E_ROCKSDB_ERROR, "rocksdb delete is failed");
  }

  return array;
}

static mrb_value
mrb_rocksdb_count(mrb_state *mrb, mrb_value self)
{
  auto handler = _rocksdb_get_handler(mrb, self);

  auto iter = handler->NewIterator(rocksdb::ReadOptions());
  iter->SeekToFirst();

  mrb_int count = 0;
  for (; iter->Valid(); iter->Next()) {
    ++count;
  }

  return mrb_fixnum_value(count);
}


static mrb_value
mrb_rocksdb_reject(mrb_state *mrb, mrb_value self)
{
  mrb_value block;
  mrb_get_args(mrb, "&", &block);

  mrb_value hash = mrb_funcall(mrb, self, "to_hash", 0);
  mrb_sym reject_sym = mrb_intern_lit(mrb, "reject");
  mrb_value rejected = mrb_funcall_with_block(mrb, hash, reject_sym, 0, NULL, block);

  return rejected;
}

/*
 * Definitions
 */
void
mrb_mruby_rocksdb_gem_init(mrb_state* mrb)
{
  struct RClass *rclass;

  rclass = mrb_define_class(mrb, CLASSNAME, mrb->object_class);

  mrb_define_method(mrb, rclass, "initialize", mrb_rocksdb_open, MRB_ARGS_REQ(3));
  mrb_define_method(mrb, rclass, "[]", mrb_rocksdb_get, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "[]=", mrb_rocksdb_set, MRB_ARGS_REQ(2));

  mrb_define_method(mrb, rclass, "close", mrb_rocksdb_close, MRB_ARGS_NONE());
  mrb_define_method(mrb, rclass, "closed?", mrb_rocksdb_closed_q, MRB_ARGS_NONE());
  mrb_define_method(mrb, rclass, "count", mrb_rocksdb_count, MRB_ARGS_NONE());
  mrb_define_method(mrb, rclass, "delete", mrb_rocksdb_delete, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "each", mrb_rocksdb_each, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "each_key", mrb_rocksdb_each_key, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "each_pair", mrb_rocksdb_each, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "each_value", mrb_rocksdb_each_value, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "fetch", mrb_rocksdb_get, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "has_key?", mrb_rocksdb_has_key_q, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "include?", mrb_rocksdb_has_key_q, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "invert", mrb_rocksdb_invert, MRB_ARGS_NONE());
  mrb_define_method(mrb, rclass, "key?", mrb_rocksdb_has_key_q, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "member?", mrb_rocksdb_has_key_q, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "open", mrb_rocksdb_open, MRB_ARGS_REQ(3));
  mrb_define_method(mrb, rclass, "reject", mrb_rocksdb_reject, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "shift", mrb_rocksdb_shift, MRB_ARGS_NONE());
  mrb_define_method(mrb, rclass, "store", mrb_rocksdb_set, MRB_ARGS_REQ(2));

  // Enumerable
  mrb_include_module(mrb, rclass, mrb_module_get(mrb, "Enumerable"));

  // Exceptions
  mrb_define_class(mrb, ROCKSDB_HANDLER_EXCEPTION, rclass);
}

void
mrb_mruby_rocksdb_gem_final(mrb_state* mrb)
{
}

