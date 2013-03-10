#!/usr/bin/env python

import os
import sys

import breakdancer
from breakdancer import Condition, Effect, Action, Driver

TESTKEY = 'testkey'

######################################################################
# Conditions
######################################################################

class ExistsCondition(Condition):

    def __call__(self, state):
        return TESTKEY in state

class ExistsAsNumber(Condition):

    def __call__(self, state):
        try:
            int(state[TESTKEY])
            return True
        except:
            return False

class MaybeExistsAsNumber(ExistsAsNumber):

    def __call__(self, state):
        return TESTKEY not in state or ExistsAsNumber.__call__(self, state)

class DoesNotExistCondition(Condition):

    def __call__(self, state):
        return TESTKEY not in state

class NothingExistsCondition(Condition):

    def __call__(self, state):
        return not bool(state)

class IsLockedCondition(Condition):

    def __call__(self, state):
        return (TESTKEY + '.lock') in state

class IsUnlockedCondition(Condition):

    def __call__(self, state):
        return (TESTKEY + '.lock') not in state

class KnowsCAS(Condition):

    def __call__(self, state):
        return (TESTKEY + '.cas') in state

######################################################################
# Effects
######################################################################

class NoFX(Effect):

    def __call__(self, state):
        pass

class MutationEffect(Effect):

    def forgetCAS(self, state):
        k = TESTKEY + '.cas'
        if k in state:
            del state[k]

class StoreEffect(MutationEffect):

    def __init__(self, rememberCAS=False):
        self.rememberCAS = rememberCAS

    def __call__(self, state):
        state[TESTKEY] = '0'
        if self.rememberCAS:
            state[TESTKEY + '.cas'] = 1
        else:
            self.forgetCAS(state)

class LockEffect(MutationEffect):

    def __call__(self, state):
        state[TESTKEY + '.lock'] = True
        self.forgetCAS(state)

class UnlockEffect(Effect):

    def __call__(self, state):
        klock = TESTKEY + '.lock'
        if klock in state:
            del state[klock]

class DeleteEffect(MutationEffect):

    def __call__(self, state):
        del state[TESTKEY]
        klock = TESTKEY + '.lock'
        if klock in state:
            del state[klock]
        self.forgetCAS(state)

class FlushEffect(Effect):

    def __call__(self, state):
        state.clear()

class AppendEffect(MutationEffect):

    suffix = '-suffix'

    def __call__(self, state):
        state[TESTKEY] = state[TESTKEY] + self.suffix
        self.forgetCAS(state)

class PrependEffect(MutationEffect):

    prefix = 'prefix-'

    def __call__(self, state):
        state[TESTKEY] = self.prefix + state[TESTKEY]
        self.forgetCAS(state)

class ArithmeticEffect(MutationEffect):

    default = '0'

    def __init__(self, by=1):
        self.by = by

    def __call__(self, state):
        if TESTKEY in state:
            state[TESTKEY] = str(max(0, int(state[TESTKEY]) + self.by))
        else:
            state[TESTKEY] = self.default
        self.forgetCAS(state)

######################################################################
# Actions
######################################################################

class Get(Action):

    preconditions = [ExistsCondition()]
    effect = NoFX()

class Set(Action):

    preconditions = [IsUnlockedCondition()]
    effect = StoreEffect()
    postconditions = [ExistsCondition()]

class setUsingCAS(Action):

    preconditions = [KnowsCAS(), IsUnlockedCondition()]
    effect = StoreEffect()
    postconditions = [ExistsCondition()]

class SetRetainCAS(Action):

    preconditions = [IsUnlockedCondition()]
    effect = StoreEffect(True)
    postconditions = [ExistsCondition(), KnowsCAS()]

class Add(Action):

    preconditions = [DoesNotExistCondition(), IsUnlockedCondition()]
    effect = StoreEffect()
    postconditions = [ExistsCondition()]

class Delete(Action):

    preconditions = [ExistsCondition(), IsUnlockedCondition()]
    effect = DeleteEffect()
    postconditions = [DoesNotExistCondition()]

class DeleteUsingCAS(Action):

    preconditions = [KnowsCAS(), ExistsCondition(), IsUnlockedCondition()]
    effect = DeleteEffect()
    postconditions = [DoesNotExistCondition()]

# class Flush(Action):

#     effect = FlushEffect()
#     postconditions = [NothingExistsCondition()]

# class WaitForLock(Action):

#     effect = UnlockEffect()

# class GetLock(Action):

#     preconditions = [ExistsCondition(), IsUnlockedCondition()]
#     effect = LockEffect()
#     postconditions = [IsLockedCondition()]

class Delay(Action):
    effect = FlushEffect()
    postconditions = [NothingExistsCondition()]

class Append(Action):

    preconditions = [ExistsCondition(), IsUnlockedCondition()]
    effect = AppendEffect()
    postconditions = [ExistsCondition()]

class Prepend(Action):

    preconditions = [ExistsCondition(), IsUnlockedCondition()]
    effect = PrependEffect()
    postconditions = [ExistsCondition()]

class AppendUsingCAS(Action):

    preconditions = [KnowsCAS(), ExistsCondition(), IsUnlockedCondition()]
    effect = AppendEffect()
    postconditions = [ExistsCondition()]

class PrependUsingCAS(Action):

    preconditions = [KnowsCAS(), ExistsCondition(), IsUnlockedCondition()]
    effect = PrependEffect()
    postconditions = [ExistsCondition()]

class Incr(Action):

    preconditions = [ExistsAsNumber(), IsUnlockedCondition()]
    effect = ArithmeticEffect(1)
    postconditions = [ExistsAsNumber()]

class Decr(Action):

    preconditions = [ExistsAsNumber(), IsUnlockedCondition()]
    effect = ArithmeticEffect(-1)
    postconditions = [ExistsAsNumber()]

class IncrWithDefault(Action):

    preconditions = [MaybeExistsAsNumber(), IsUnlockedCondition()]
    effect = ArithmeticEffect(1)
    postconditions = [ExistsAsNumber()]

class DecrWithDefault(Action):

    preconditions = [MaybeExistsAsNumber(), IsUnlockedCondition()]
    effect = ArithmeticEffect(-1)
    postconditions = [ExistsAsNumber()]

######################################################################
# Driver
######################################################################

class TestFile(object):

    def __init__(self, path, n=10):
        if n == 1:
            self.tmpfilenames = ["%s_test.json.tmp" % path]
        else:
            self.tmpfilenames = ["%s_%d_test.json.tmp" % (path, i) for i in range(n)]
        self.files = [open(tfn, "w") for tfn in self.tmpfilenames]
        self.seq = [list() for f in self.files]
        self.index = 0

    def finish(self):
        for f in self.files:
            f.close()

        for tfn in self.tmpfilenames:
            nfn = tfn[:-4]
            assert (nfn + '.tmp') == tfn
            os.rename(tfn, nfn)

    def nextfile(self):
        self.index = self.index + 1
        if self.index >= len(self.files):
            self.index = 0

    def write(self, s):
        self.files[self.index].write(s)

    def addseq(self, seq):
        self.seq[self.index].append(seq)

class EngineTestAppDriver(Driver):

    def __init__(self, writer=sys.stdout):
        self.writer = writer

    def output(self, s):
        self.writer.write(s)

    def preSuite(self, seq):
        files = [self.writer]
        if isinstance(self.writer, TestFile):
            files = self.writer.files

    def testName(self, seq):
        return ' -> '.join(a.name for a in seq)

    def shouldRunSequence(self, seq):
        # Skip any sequence that leads to known failures
        ok = True
        sclasses = [type(a) for a in seq]
        # A list of lists of classes such that any test that includes
        # the inner list in order should be skipped.
        bads = []
        for b in bads:
            try:
                nextIdx = 0
                for c in b:
                    nextIdx = sclasses.index(c, nextIdx) + 1
                ok = False
            except ValueError:
                pass # Didn't find it, move in

        return ok

    def startSequence(self, seq):
        if isinstance(self.writer, TestFile):
            self.writer.nextfile()
            self.writer.addseq(seq)

        f = '{\n  "%s": [\n' % self.testName(seq)
        self.output(f)

        self.handled = self.shouldRunSequence(seq)

        if not self.handled:
            self.output("}\n\n")

    def startAction(self, action):
        if not self.handled:
            return

        if isinstance(action, Delay):
            s = '      {"op": "delay"'
        # elif isinstance(action, WaitForLock):
        #     s = "    delay(locktime+1)"
        # elif isinstance(action, Flush):
        #     s = "    flush(b)"
        elif isinstance(action, Delete):
            s = '      {"op": "del"'
        else:
            s = '      {"op": "%s"' % (action.name)
        self.output(s)

    def postSuite(self, seq):
        pass

    def endSequence(self, seq, state):
        if not self.handled:
            return

        val = state.get(TESTKEY)
        if val:
            self.output('      {"op": "assert", "val": "%s"}\n  ]\n' % val)
        else:
            self.output('      {"op": "assertMissing"}\n  ]\n')
        self.output("}\n")

    def endAction(self, action, state, errored):
        if not self.handled:
            return

        value = state.get(TESTKEY)
        if value:
            self.output(', "val": "%s"' % value)
        else:
            pass
            # vs = ' // value is not defined\n'

        if errored:
            self.output(', "error": true},\n')
        else:
            self.output(', "error": false},\n')

if __name__ == '__main__':
    w = TestFile('generated_suite', 1)
    breakdancer.runTest(breakdancer.findActions(globals().values()),
                        EngineTestAppDriver(w))
    w.finish()
