﻿#include <lmdb.h>
#include <mruby.h>
#include <mruby/data.h>
#include <mruby/class.h>
#include <mruby/string.h>
#include <mruby/variable.h>
#include <mruby/array.h>

#define LMDB_STAT mrb_class_get_under(mrb, mrb_module_get(mrb, "MDB"), "Stat")
#define LMDB_ENV_INFO mrb_class_get_under(mrb, mrb_class_get_under(mrb, mrb_module_get(mrb, "MDB"), "Env"), "Info")
