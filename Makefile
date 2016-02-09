PROJECT = yamq

# Options ##############################################################
COMPILE_FLAGS = -DS2_USE_LAGER
EUNIT_OPTS    = [verbose]
ERLC_OPTS    ?= -Werror +debug_info +warn_export_all +warn_export_vars \
                +warn_shadow_vars +warn_obsolete_guard +warnings_as_errors \
                -DS2_USE_LAGER

# Dependecies ##########################################################
DEPS = lager stdlib2

dep_lager   = git git@github.com:kivra/lager.git   3.0.1
dep_stdlib2 = git git@github.com:kivra/stdlib2.git master

# Standard targets #####################################################
include erlang.mk

# eof
