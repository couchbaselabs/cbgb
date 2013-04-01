//
// _sum
//

assert("builtin_sum", _sum(["a", "b"], [1, 2, 3], false), 6);
assert("builtin_sum rere", _sum(["a", "b"], [1, 2, 3], true), 6);

//
// _count
//

assert("builtin_reduce _count", _count(["a", "b"], [1, 2, 3], false), 3);
assert("builtin_reduce _count rere", _count(["a", "b"], [1, 2, 3], true), 6);

//
// _stats
//

// First, if we pass it nothing, does it freak out?
assert("builtin_stats empty", _stats([], [], false).count, 0);

// Try a single, three item pass and verify all results.
var statsed = _stats(["a", "b", "c"],
                     [2, 5, 6],
                     false);

assert("builtin_stats count", statsed.count, 3);
assert("builtin_stats sum", statsed.sum, 13);
assert("builtin_stats sumsqr", statsed.sumsqr, 65);
assert("builtin_stats min", statsed.min, 2);
assert("builtin_stats max", statsed.max, 6);

// Now we get fancy, two passes, different sizes of items, verify results.
var statsets = [[1, 2], [2, 9], [19, 19, 187]];

var firstpasses = [];
for (var i = 0; i < statsets.length; i++) {
    firstpasses.push(_stats(["a", "b"], statsets[i], false));
}

statsed = _stats(["a", "b"], firstpasses, true);

assert("builtin_stats rere count", statsed.count, 7);
assert("builtin_stats rere sum", statsed.sum, 239);
assert("builtin_stats rere sumsqr", statsed.sumsqr, 35781);
assert("builtin_stats rere min", statsed.min, 1);
assert("builtin_stats rere max", statsed.max, 187);
