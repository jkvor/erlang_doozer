{application, erlang_doozer,
 [
  {description, ""},
  {vsn, "1"},
  {registered, []},
  {modules, [
             erlang_doozer_app,
             erlang_doozer_sup
            ]},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {mod, { erlang_doozer_app, []}},
  {env, []}
 ]}.
