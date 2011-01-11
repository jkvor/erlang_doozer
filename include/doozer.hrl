-author('Arun Suresh <arun.suresh@gmail.com>').

-record(request, {tag = 0, 
                  verb, 
                  cas, 
                  path, 
                  value,
                  id,
                  offset,
                  limit}).

-record(response, {tag, 
                   flags, 
                   seqn, 
                   cas, 
                   path,
                   value,
                   id,
                   err_code,
                   err_detail}).
