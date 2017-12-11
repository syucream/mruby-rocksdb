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

/*
static mrb_value
mrb_rocksdb_each(mrb_state *mrb, mrb_value self)
{
  mrb_value block;
  mrb_get_args(mrb, "&", &block);

  unsigned char *ck	= NULL, *cv = NULL;
  size_t klen = 0, vlen = 0;
  k2h_h handler = _rocksdb_get_handler(mrb, self);

  ROCKSDB_ITER_BEGIN(mrb, handler, ck, klen, cv, vlen);
  {
    mrb_value key = mrb_str_new(mrb, (char*)ck, klen);
    mrb_value val = mrb_str_new(mrb, (char*)cv, vlen);
    mrb_value argv[PAIR_ARGC] = {key, val};
    mrb_yield_argv(mrb, block, PAIR_ARGC, argv);
  }
  ROCKSDB_ITER_END(mrb, ck, cv);

  return self;
}

static mrb_value
mrb_rocksdb_each_key(mrb_state *mrb, mrb_value self)
{
  mrb_value block;
  mrb_get_args(mrb, "&", &block);

  unsigned char *ck	= NULL, *cv = NULL;
  size_t klen = 0, vlen = 0;
  k2h_h handler = _rocksdb_get_handler(mrb, self);

  ROCKSDB_ITER_BEGIN(mrb, handler, ck, klen, cv, vlen);
  {
    mrb_value key = mrb_str_new(mrb, (char*)ck, klen);
    mrb_yield(mrb, block, key);
  }
  ROCKSDB_ITER_END(mrb, ck, cv);

  return self;
}

static mrb_value
mrb_rocksdb_each_value(mrb_state *mrb, mrb_value self)
{
  mrb_value block;
  mrb_get_args(mrb, "&", &block);

  unsigned char *ck	= NULL, *cv = NULL;
  size_t klen = 0, vlen = 0;
  k2h_h handler = _rocksdb_get_handler(mrb, self);

  ROCKSDB_ITER_BEGIN(mrb, handler, ck, klen, cv, vlen);
  {
    mrb_value value = mrb_str_new(mrb, (char*)cv, vlen);
    mrb_yield(mrb, block, value);
  }
  ROCKSDB_ITER_END(mrb, ck, cv);

  return self;
}

static mrb_value
mrb_rocksdb_empty_q(mrb_state *mrb, mrb_value self)
{
  k2h_h handler = _rocksdb_get_handler(mrb, self);
  bool is_empty = k2h_find_first(handler) == K2H_INVALID_HANDLE;
  return is_empty ? mrb_true_value() : mrb_false_value();
}

static mrb_value
mrb_rocksdb_has_key_q(mrb_state *mrb, mrb_value self)
{
  bool has_key = !mrb_nil_p(mrb_rocksdb_get(mrb, self));
  return has_key ? mrb_true_value() : mrb_false_value();
}

static mrb_value
mrb_rocksdb_has_value_q(mrb_state *mrb, mrb_value self)
{
  mrb_value target;
  mrb_get_args(mrb, "S", &target);

  unsigned char *ck	= NULL, *cv = NULL;
  size_t klen = 0, vlen = 0;
  k2h_h handler = _rocksdb_get_handler(mrb, self);

  mrb_value found = mrb_false_value();
  ROCKSDB_ITER_BEGIN(mrb, handler, ck, klen, cv, vlen);
  {
    mrb_value val = mrb_str_new(mrb, (char*)cv, vlen);
    if (mrb_str_equal(mrb, val, target)) {
      found = mrb_true_value();
    }
  }
  ROCKSDB_ITER_END(mrb, ck, cv);

  return found;
}

static mrb_value
mrb_rocksdb_clear(mrb_state *mrb, mrb_value self)
{
  k2h_h handler = _rocksdb_get_handler(mrb, self);

  _rocksdb_clear(mrb, handler);

  return self;
}
*/

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

/*
static mrb_value
mrb_rocksdb_delete_if(mrb_state *mrb, mrb_value self)
{
  mrb_value block;
  mrb_get_args(mrb, "&", &block);

  unsigned char *ck	= NULL, *cv = NULL;
  size_t klen = 0, vlen = 0;
  k2h_h handler = _rocksdb_get_handler(mrb, self);

  ROCKSDB_ITER_BEGIN(mrb, handler, ck, klen, cv, vlen);
  {
    mrb_value key = mrb_str_new(mrb, (char*)ck, klen);
    mrb_value val = mrb_str_new(mrb, (char*)cv, vlen);
    mrb_value argv[PAIR_ARGC] = {key, val};
    mrb_value rc = mrb_yield_argv(mrb, block, PAIR_ARGC, argv);

    if (mrb_bool(rc)) {
      failed = !(k2h_remove_all(handler, ck, klen));
    }
  }
  ROCKSDB_ITER_END(mrb, ck, cv);

  return self;
}

static mrb_value
mrb_rocksdb_invert(mrb_state *mrb, mrb_value self)
{
  unsigned char *ck	= NULL, *cv = NULL;
  size_t klen = 0, vlen = 0;
  k2h_h handler = _rocksdb_get_handler(mrb, self);

  mrb_value hash = mrb_hash_new(mrb);

  ROCKSDB_ITER_BEGIN(mrb, handler, ck, klen, cv, vlen);
  {
    mrb_value key = mrb_str_new(mrb, (char*)ck, klen);
    mrb_value val = mrb_str_new(mrb, (char*)cv, vlen);
    mrb_hash_set(mrb, hash, val, key);
  }
  ROCKSDB_ITER_END(mrb, ck, cv);

  return hash;
}

static mrb_value
mrb_rocksdb_values_at(mrb_state *mrb, mrb_value self)
{
  mrb_value *argv;
  mrb_int argc;
  mrb_get_args(mrb, "*", &argv, &argc);

  k2h_h handler = _rocksdb_get_handler(mrb, self);
  mrb_value array = mrb_ary_new(mrb);

  for (int i = 0; i<argc; i++) {
    mrb_value str = argv[i];
    if (!mrb_string_p(str)) {
      continue;
    }

    const char* pkey = mrb_string_value_ptr(mrb, str);
    size_t keylen = mrb_string_value_len(mrb, str);
    unsigned char* pval = NULL;
    size_t vallen = 0;
    bool found = k2h_get_value(handler, (unsigned char*)pkey, keylen, &pval, &vallen);

    if (found) {
      mrb_value v = mrb_str_new(mrb, (char*)pval, vallen);
      mrb_ary_push(mrb, array, v);
    }

    free(pval);
  }

  return array;
}

static mrb_value
mrb_rocksdb_shift(mrb_state *mrb, mrb_value self)
{
  k2h_h handler = _rocksdb_get_handler(mrb, self);
  k2h_find_h fh = k2h_find_first(handler);

  if (fh == K2H_INVALID_HANDLE) {
    return mrb_nil_value();
  }

  unsigned char *ck	= NULL, *cv = NULL;
  size_t klen = 0, vlen = 0;
  bool failed = false;

  mrb_value array = mrb_ary_new(mrb);
  if (k2h_find_get_key(fh, &ck, &klen) && k2h_find_get_value(fh, &cv, &vlen)) {
    mrb_value key = mrb_str_new(mrb, (char*)ck, klen);
    mrb_value val = mrb_str_new(mrb, (char*)cv, vlen);
    mrb_ary_push(mrb, array, key);
    mrb_ary_push(mrb, array, val);
    failed = !(k2h_remove_all(handler, ck, klen));
  } else {
    failed = true;
  }
  free(ck);
  free(cv);
  k2h_find_free(fh);

  if (failed) {
    mrb_raise(mrb, E_ROCKSDB_ERROR, "k2h_find failed");
    return mrb_nil_value();
  } else {
    return array;
  }
}

static mrb_value
mrb_rocksdb_count(mrb_state *mrb, mrb_value self)
{
  k2h_h handler = _rocksdb_get_handler(mrb, self);

  mrb_int count = 0;
  for (k2h_find_h fh = k2h_find_first(handler); K2H_INVALID_HANDLE != fh; fh = k2h_find_next(fh)) {
    ++count;
  }

  return mrb_fixnum_value(count);
}

static mrb_value
mrb_rocksdb_to_hash(mrb_state *mrb, mrb_value self)
{
  unsigned char *ck	= NULL, *cv = NULL;
  size_t klen = 0, vlen = 0;
  k2h_h handler = _rocksdb_get_handler(mrb, self);

  mrb_value hash = mrb_hash_new(mrb);
  ROCKSDB_ITER_BEGIN(mrb, handler, ck, klen, cv, vlen);
  {
    mrb_value key = mrb_str_new(mrb, (char*)ck, klen);
    mrb_value val = mrb_str_new(mrb, (char*)cv, vlen);
    mrb_hash_set(mrb, hash, key, val);
  }
  ROCKSDB_ITER_END(mrb, ck, cv);

  return hash;
}
*/

/*
 * Enumerable methods
 */
/*
static mrb_value
_mrb_rocksdb_keys_map(mrb_state *mrb, mrb_value v)
{
  mrb_value key, val;
  mrb_get_args(mrb, "SS", &key, &val);
  return key;
}
static mrb_value
mrb_rocksdb_keys(mrb_state *mrb, mrb_value self)
{
  struct RProc* proc = mrb_proc_new_cfunc(mrb, _mrb_rocksdb_keys_map);
  mrb_value block = mrb_obj_value(proc);
  return mrb_funcall_with_block(mrb, self, mrb_intern_lit(mrb, "map"), 0, NULL, block);
}

static mrb_value
_mrb_rocksdb_values_map(mrb_state *mrb, mrb_value v)
{
  mrb_value key, val;
  mrb_get_args(mrb, "SS", &key, &val);
  return val;
}
static mrb_value
mrb_rocksdb_values(mrb_state *mrb, mrb_value self)
{
  struct RProc* proc = mrb_proc_new_cfunc(mrb, _mrb_rocksdb_values_map);
  mrb_value block = mrb_obj_value(proc);
  return mrb_funcall_with_block(mrb, self, mrb_intern_lit(mrb, "map"), 0, NULL, block);
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
*/

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

  // mrb_define_method(mrb, rclass, "clear", mrb_rocksdb_clear, MRB_ARGS_NONE());
  mrb_define_method(mrb, rclass, "close", mrb_rocksdb_close, MRB_ARGS_NONE());
  mrb_define_method(mrb, rclass, "closed?", mrb_rocksdb_closed_q, MRB_ARGS_NONE());
  /*
  mrb_define_method(mrb, rclass, "count", mrb_rocksdb_count, MRB_ARGS_NONE());
  mrb_define_method(mrb, rclass, "delete", mrb_rocksdb_delete, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "delete_if", mrb_rocksdb_delete_if, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "each", mrb_rocksdb_each, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "each_key", mrb_rocksdb_each_key, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "each_pair", mrb_rocksdb_each, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "each_value", mrb_rocksdb_each_value, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "empty?", mrb_rocksdb_empty_q, MRB_ARGS_NONE());
  mrb_define_method(mrb, rclass, "fetch", mrb_rocksdb_get, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "has_key?", mrb_rocksdb_has_key_q, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "has_value?", mrb_rocksdb_has_value_q, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "include?", mrb_rocksdb_has_key_q, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "invert", mrb_rocksdb_invert, MRB_ARGS_NONE());
  mrb_define_method(mrb, rclass, "key?", mrb_rocksdb_has_key_q, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "member?", mrb_rocksdb_has_key_q, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "open", mrb_rocksdb_open, MRB_ARGS_REQ(3));
  mrb_define_method(mrb, rclass, "reject", mrb_rocksdb_reject, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "reject!", mrb_rocksdb_delete_if, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "shift", mrb_rocksdb_shift, MRB_ARGS_NONE());
  mrb_define_method(mrb, rclass, "store", mrb_rocksdb_set, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, rclass, "to_hash", mrb_rocksdb_to_hash, MRB_ARGS_NONE());
  mrb_define_method(mrb, rclass, "value?", mrb_rocksdb_has_value_q, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, rclass, "values_at", mrb_rocksdb_values_at, MRB_ARGS_ANY());

  // Enumerable
  mrb_include_module(mrb, rclass, mrb_module_get(mrb, "Enumerable"));
  mrb_define_method(mrb, rclass, "keys", mrb_rocksdb_keys, MRB_ARGS_NONE());
  mrb_define_method(mrb, rclass, "values", mrb_rocksdb_values, MRB_ARGS_NONE());
  */

  // Exceptions
  mrb_define_class(mrb, ROCKSDB_HANDLER_EXCEPTION, rclass);
}

void
mrb_mruby_rocksdb_gem_final(mrb_state* mrb)
{
}

