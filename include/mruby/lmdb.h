﻿#ifndef MRUBY_LMDB_H
#define MRUBY_LMDB_H

#include <mruby.h>

#ifdef MRB_INT16
# error MRB_INT16 is too small for mruby-lmdb.
#endif

MRB_BEGIN_DECL

#define E_LMDB_ERROR (mrb_class_get_under(mrb, mrb_module_get(mrb, "MDB"), "Error"))

MRB_END_DECL

#endif
