assert("basic sum", sum(1, 2, 3), 6);
assert("empty sum", sum(), 0);
assert("NaN sum", sum(1, NaN, 1), 2);
assert("array sum", sum([1, 2, 3]), 6);

assert("builtin_sum", _sum(["a", "b"], [1, 2, 3], false), 6);
assert("builtin_sum rere", _sum(["a", "b"], [1, 2, 3], true), 6);

assert("builtin_reduce", _count(["a", "b"], [1, 2, 3], false), 3);
assert("builtin_reduce rere", _count(["a", "b"], [1, 2, 3], true), 6);
