%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright Bjorn Jensen-Urstad 2013
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(yamq_store).

%%%_* Includes =========================================================
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Code =============================================================
%%%_ * Behaviour -------------------------------------------------------
-callback list()                  -> maybe(list(), _).
-callback get(any())              -> maybe(any(), _).
-callback put(any(), any())       -> whynot(_).
-callback delete(any())           -> whynot(_).
-callback decode_key(yamq:key())  -> yamq:info().
-callback encode_key(yamq:info()) -> yamq:key().
-callback generate_info(yamq:priority(), yamq:due(), binary()) -> yamq:info().

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
