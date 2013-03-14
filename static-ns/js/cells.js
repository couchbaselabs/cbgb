/**
   Copyright 2011 Couchbase, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 **/
var Slave = mkClass({
  initialize: function (thunk) {
    this.thunk = thunk
  },
  die: function () {this.dead = true;},
  nMoreTimes: function (times) {
    this.times = this.times || 0;
    this.times += times;
    var oldThunk = this.thunk;
    this.thunk = function (data) {
      oldThunk.call(this, data);
      if (--this.times == 0)
        this.die();
    }
    return this;
  }
});

var CallbackSlot = mkClass({
  initialize: function () {
    this.slaves = [];
    this.broadcasting = 0;
  },
  subscribeWithSlave: function (thunkOrSlave) {
    var slave;
    if (thunkOrSlave instanceof Slave)
      slave = thunkOrSlave;
    else
      slave = new Slave(thunkOrSlave);
    var wasEmpty = (this.slaves.length == 0);
    this.slaves.push(slave);
    if (wasEmpty)
      this.__demandChanged(true);
    return slave;
  },
  subscribeOnce: function (thunk) {
    return this.subscribeWithSlave(thunk).nMoreTimes(1);
  },
  broadcast: function (data) {
    this.broadcasting++;
    _.each(this.slaves, function (slave) {
      if (slave.dead)
        return;
      try {
        slave.thunk(data);
      } catch (e) {
        console.log("got exception in CallbackSlot#broadcast", e, "for slave thunk", slave.thunk);
        slave.die();
        _.defer(function () {throw e;});
      }
    });
    this.broadcasting--;
    this.cleanup();
  },
  unsubscribeCallback: function (thunk) {
    var slave = _.detect(this.slaves, function (candidate) {
      return candidate.thunk == thunk;
    });
    if (slave)
      this.unsubscribe(slave);
    return slave;
  },
  unsubscribe: function (slave) {
    slave.die();
    if (this.broadcasting)
      return;
    var index = $.inArray(slave, this.slaves);
    if (index >= 0) {
      this.slaves.splice(index, 1);
      if (!this.slaves.length)
        this.__demandChanged(false);
    }
  },
  cleanup: function () {
    if (this.broadcasting)
      return;
    var oldLength = this.slaves.length;
    this.slaves = _.reject(this.slaves, function (slave) {return slave.dead;});
    if (oldLength && !this.slaves.length)
      this.__demandChanged(false);
  },
  __demandChanged: function (haveDemand) {
  }
});

// returns special value that when passed to Cell#setValue initiates async set
// 'body' is a function that's passed cell-generated dataCallback
// 'body' is assumed to arrange async process of computing/getting new value
// 'body' should arrange call to given dataCallback when new value is available, passing it this new value
// Value returned from body is ignored
//
// see future.get for usage example
function future(body, options) {
  return new Future(body, options);
}
function Future(body, options) {
  this.thunk = body;
  _.extend(this, options || {});
}
Future.prototype = {
  constructor: Future,
  cancelled: false,
  removeNowValue: function () {
    var rv = this.nowValue;
    delete this.nowValue;
    return rv;
  },
  mkCallback: function (cell) {
    var async = this;
    var rv = function (data) {
      if (async.action)
        async.action.finish();
      return cell.deliverFutureValue(async, data);
    }
    rv.async = async;
    rv.cell = cell;
    rv.continuing = function (data) {
      async.nowValue = data;
      return rv(async);
    }
    return rv;
  },
  start: function (cell) {
    if (this.modal) {
      this.action = new ModalAction();
    }
    this.started = true;
    var thunk = this.thunk;
    this.thunk = undefined;
    thunk.call(cell.mkFormulaContext(), this.mkCallback(cell));
  }
};

// future.wrap allows creation of 'future wrappers' that allow you to
// run your code before/after future is started and before/after
// future value is delivered. It's even possible to block value delivery.
//
// wrapperBody argument is function that will be called instead of
// real future body real future is started. It accepts dataCallback
// from real future and startInner function. By calling startInner it
// can execute real future body. dataCallback (real or wrapper) needs
// to be passed to that function. By passing your wrapper it's
// possible to execute arbitrary code around future value delivery.
future.wrap = function (wrapperBody, maybeFuture) {
  maybeFuture = maybeFuture || future;
  return function (innerBody, futureOptions) {
    return maybeFuture(function (realDataCallback) {
      var context = this;
      function startInner(innerDataCallbackArg, dontReplace) {
        if (!dontReplace) {
          innerDataCallbackArg.cell = realDataCallback.cell;
          innerDataCallbackArg.async = realDataCallback.async;
          innerDataCallbackArg.continuing = innerDataCallbackArg.continuing;
        }
        return innerBody.call(context, innerDataCallbackArg);
      }
      return wrapperBody.call(this, realDataCallback, startInner);
    }, futureOptions);
  }
};

// inspired in part by http://common-lisp.net/project/cells/
var Cell = mkClass({
  initialize: function (formula, sources) {
    this.changedSlot = new CallbackSlot();
    this.undefinedSlot = new CallbackSlot();
    this.dependenciesSlot = new CallbackSlot();
    this.formula = formula;
    this.effectiveFormula = formula;
    this.value = undefined;
    this.sources = [];
    this.context = {};
    this.argumentSourceNames = [];
    if (sources)
      this.setSources(sources);
    else if (this.formula)
      this.recalculate();
    this.createdAt = (function () {
      try {
        throw new Error();
      } catch (e) {
        return e;
      }
    })();
  },
  equality: function (a, b) {
    return a == b;
  },
  subscribe: function (cb, options) {
    options = _.extend({'undefined': false,
                        changed: true}, options || {});
    var slave = new Slave(cb);
    if (options["undefined"])
      this.undefinedSlot.subscribeWithSlave(slave);
    if (options.changed)
      this.changedSlot.subscribeWithSlave(slave);
    return slave;
  },
  subscribeOnUndefined: function (cb) {
    return this.subscribe(cb, {'undefined': true, changed: false});
  },
  subscribeAny: function (cb) {
    return this.subscribe(cb, {'undefined': true});
  },
  setSources: function (context) {
    var self = this;
    if (this.sources.length != 0)
      throw new Error('cannot adjust sources yet');
    if (!this.formula)
      throw new Error("formula-less cells cannot have sources");
    var cells = this.sources = _.values(context);
    this.context = _.extend({self: this}, context);
    if (_.any(cells, function (v) {return v == null;})) {
      var badSources = [];
      _.each(this.context, function (v, k) {
        if (!v)
          badSources.push(k);
      });
      throw new Error("null for following source cells: " + badSources.join(', '));
    }

    var recalculate = $m(self, 'recalculate');
    _.each(cells, function (cell) {
      cell.dependenciesSlot.subscribeWithSlave(recalculate);
    });

    self.boundRecalculate = recalculate;

    var argumentSourceNames = this.argumentSourceNames = functionArgumentNames(this.formula);
    _.each(this.argumentSourceNames, function (a) {
      if (!(a in context))
        throw new Error('missing source named ' + a + ' which is required for formula:' + self.formula);
    });
    if (argumentSourceNames.length)
      this.effectiveFormula = this.mkEffectiveFormula();

    this.recalculate();
    return this;
  },
  getSourceCells: function () {
    return this.sources;
  },
  mkEffectiveFormula: function () {
    var argumentSourceNames = this.argumentSourceNames;
    var formula = this.formula;
    return function () {
      var notOk = false;
      var self = this;
      var requiredValues = _.map(argumentSourceNames, function (a) {
        var rv = self[a];
        if (rv === undefined) {
          notOk = true;
        }
        return rv;
      });
      if (notOk)
        return;
      return formula.apply(this, requiredValues);
    }
  },
  // applies f to current cell value and extra arguments
  // and sets value to it's return value
  modifyValue: function (f) {
    var extra = _.rest(arguments);
    this.setValue(f.apply(null, [this.value].concat(extra)));
  },
  _markForCompletion: function () {
    if (Cell.recalcCount == 0) {
      Cell.completeCellDelay(this);
      return;
    }

    if (this.recalcGeneration != Cell.recalcGeneration) {
      this.recalcGeneration = Cell.recalcGeneration;
      Cell.updatedCells.push(this);
    }
  },
  isValuesDiffer: function (oldValue, newValue) {
    if (newValue === undefined) {
      if (oldValue == undefined)
        return false;
    } else {
      if (oldValue !== undefined && this.equality(oldValue, newValue))
        return false;
    }

    return true;
  },
  setValue: function (newValue) {
    this.cancelAsyncSet();
    this.resetRecalculateAt();

    if (newValue instanceof Future) {
      var async = newValue;
      if (this.keepValueDuringAsync) {
        newValue = this.value;
      } else {
        newValue = async.removeNowValue();
      }
      this.pendingFuture = async;
      this._markForCompletion();
    }

    var oldValue = this.value;
    if (this.beforeChangeHook)
      newValue = this.beforeChangeHook(newValue);
    this.value = newValue;

    if (!this.isValuesDiffer(oldValue, newValue))
      return;

    this.dependenciesSlot.broadcast(this);

    if (Cell.recalcCount == 0) {
      // if everything is stable, notify watchers now
      notifyWatchers.call(this, newValue);
    } else {
      // if there are pending slot recomputations -- delay
      if (!this.delayedBroadcast)
        this.delayedBroadcast = notifyWatchers;
      this._markForCompletion();
    }

    return true;

    function notifyWatchers(newValue) {
      if (newValue === undefined) {
        if (oldValue !== undefined)
          this.undefinedSlot.broadcast(this);
        return;
      }

      if (!this.equality(oldValue, newValue))
        this.changedSlot.broadcast(this);
    }
  },
  // 'returns' value in continuation-passing style way. Calls body
  // with cell's value. If value is undefined, calls body when value
  // becomes defined. Returns undefined
  getValue: function (body) {
    if (this.value)
      body(this.value);
    else
      this.changedSlot.subscribeOnce(function (self) {
        body(self.value);
      });
  },
  // continuus getValue. Will call cb now and every time the value changes,
  // passing it to cb.
  subscribeValue: function (cb) {
    var cell = this;
    var slave = this.subscribeAny(function () {
      cb(cell.value);
    });
    cb(cell.value);

    return {
      cancel: function () {
        cell.undefinedSlot.unsubscribe(slave);
        cell.changedSlot.unsubscribe(slave);
      }
    }
  },
  // schedules cell value recalculation
  recalculate: function () {
    if (this.queuedValueUpdate)
      return;
    this.resetRecalculateAt();
    Cell.recalcCount++;
    Cell.planUpdate(this);
    this.queuedValueUpdate = true;
  },
  // forces cell recalculation unless async set is in progress
  // recalculate() call would abort and re-issue in-flight XHR
  // request, which is almost always bad thing.
  //
  // If in-progress future is marked as weak then we force
  // recalculation. Weak futures can be handy in same cases, in
  // particular during network error recovery, where invalidate() call
  // should force new network request.
  invalidate: function (callback) {
    if (callback)
      this.changedSlot.subscribeOnce(callback);
    if (this.pendingFuture && !this.pendingFuture.weak)
      return;
    this.recalculate();
  },
  mkFormulaContext: function () {
    var context = {};
    _.each(this.context, function (cell, key) {
      context[key] = (key == 'self') ? cell : cell.value;
    });
    return context;
  },
  tryUpdatingValue: function () {
    try {
      var context = this.mkFormulaContext();
      var value = this.effectiveFormula.call(context);
      this.setValue(value);
    } finally {
      this.queuedValueUpdate = false;
      if (--Cell.recalcCount == 0)
        Cell.completeGeneration();
    }
  },
  deliverFutureValue: function (future, value) {
    // detect cancellation
    if (this.pendingFuture != future)
      return false;

    this.pendingFuture = null;

    if (future.valueTransformer && !(value instanceof Future))
      value = (future.valueTransformer)(value);

    this.setValue(value);
    return true;
  },
  cancelAsyncSet: function () {
    var async = this.pendingFuture;
    if (!async)
      return;
    this.pendingFuture = null;
    async.cancelled = true;
    if (async.started && async.cancel) {
      try {
        async.cancel();
      } catch (e) {
        setTimeout(function () {throw e}, 0);
      }
    }
  },
  resetRecalculateAt: function () {
    this.recalculateAtTime = undefined;
    if (this.recalculateAtTimeout)
      clearTimeout(this.recalculateAtTimeout);
    this.recalculateAtTimeout = undefined;
  },
  recalculateAt: function (time) {
    if (time instanceof Date)
      time = time.valueOf();

    if (this.recalculateAtTime) {
      if (this.recalculateAtTime < time)
        return;
      clearTimeout(this.recalculateAtTimeout);
      this.recalculateAtTimeout = undefined;
    }
    this.recalculateAtTime = time;

    var delay = time - (new Date()).valueOf();

    if (delay <= 0)
      this.invalidate();
    else
      // yes we re-check current time after delay
      // as I've seen few cases where browsers run callback earlier by few milliseconds
      this.recalculateAtTimeout = setTimeout(_.bind(this.recalculateAt, this, time), delay);
  },
  recalculateAfterDelay: function (delayMillis) {
    var time = (new Date()).valueOf();
    time += delayMillis;
    this.recalculateAt(time);
  },
  // modifies some (potentially nested) attribute of value
  // makes sure that old version of value doesn't notice the change
  setValueAttr: function (attrValue, firstAttribute/*, restNestedAttributes... */) {
    var currentValue = _.clone(this.value);
    var topValue = currentValue;
    var i;
    for (i = 1; i < arguments.length - 1; i++) {
      var nextValue = _.clone(currentValue[arguments[i]]);
      currentValue[arguments[i]] = nextValue;
      currentValue = nextValue;
    }
    currentValue[arguments[arguments.length-1]] = attrValue;
    this.setValue(topValue);
  },
  // detaches this cell from it's sources making it garbage
  // collectable. If JS would have weak-pointers we could get away
  // without explicit detaching, but it doesn't yet.
  detach: function () {
    var recalculate = this.boundRecalculate;
    _.each(this.sources, function (cell) {
      cell.dependenciesSlot.unsubscribeCallback(recalculate);
    });
    clearTimeout(this.recalculateAtTimeout);
    this.recalculateAtTimeout = undefined;
  },
  name: function (name) {
    if (name) {
      this._name = name;
      return this;
    } else {
      return this._name;
    }
  },
  traceOn: function () {
    this.traceCnt = (this.traceCnt || 0) + 1;
    if (this.traceCnt != 1) {
      return this;
    }
    this.doTraceOn();
    return this;
  },
  traceOff: function () {
    // NOTE: undefined is converted to NaN here
    if (!((Number(this.traceCnt)) > 0)) {
      debugger
      throw new Error();
    }
    if (--this.traceCnt) {
      return this;
    }
    this.doTraceOff();
    return this;
  },
  effectiveName: function () {
    return this.name() || String(Cell.id(this));
  },
  doTraceOn: function () {
    var self = this;
    var tryUpdatingValue = self.tryUpdatingValue;
    var setValue = self.setValue;
    this.tryUpdatingValue = function () {
      console.log(this.effectiveName() + ": going to recompute", this.createdAt.stack);
      try {
        return tryUpdatingValue.apply(this, arguments);
      } finally {
        Cell.recordHistory(this);
      }
    }
    this.setValue = function (v) {
      try {
        var rv = setValue.apply(this, arguments);
        if (rv) {
          console.log(this.effectiveName() + ": new value: ", this.value, this.pendingFuture)
        }
        return rv;
      } finally {
        Cell.recordHistory(this);
      }
    }
  },
  doTraceOff: function () {
    delete this.tryUpdatingValue;
    delete this.setValue;
  }
});

Cell.cellComputationsHistory = [];
Cell.historyCounter = 0;
Cell.recordHistory = function (cell) {
  var history = Cell.cellComputationsHistory;
  while (history.length > 100) {
    history.shift();
  }
  history.push(cell);
  if (++Cell.historyCounter % 1000 == 0) {
    debugger
  }
}
// This is purely for debugging. Uses non-standard settable __proto__. Works
// under WebKit and Mozilla
Cell.traceEverything = function () {
  var f = function () {}
  f.prototype = Cell.prototype.__proto__;
  var superProto = new f;
  Cell.prototype.__proto__ == superProto;
  superProto.tryUpdatingValue = Cell.prototype.tryUpdatingValue;
  superProto.setValue = Cell.prototype.setValue;

  Cell.prototype.doTraceOn();

  return untraceEverything;
  function untraceEverything() {
    Cell.prototype.tryUpdatingValue = superProto.tryUpdatingValue;
    Cell.prototype.setValue = superProto.setValue;
    Cell.prototype.__proto__ = superProto.__proto__;
  }
}

_.extend(Cell, {
  EMPTY_OBJECT: {},
  updatedCells: [],
  recalcGeneration: {},
  recalcCount: 0,
  pendingUpdates: [],
  forgetState: function () {
    Cell.updatedCells = [];
    Cell.pendingUpdates = [];
    Cell.recalcGeneration = {};
    Cell.recalcCount = 0;
  },
  planUpdate: function (cell) {
    var pendingUpdates = Cell.pendingUpdates;
    pendingUpdates.push(cell);
    if (pendingUpdates.length === 1) {
      setTimeout(Cell.invokeUpdates, 0);
    }
  },
  invokeUpdates: function () {
    var limit = 32;
    var pendingUpdates = Cell.pendingUpdates;
    while (pendingUpdates.length && limit--) {
      var cell = pendingUpdates.shift();
      cell.tryUpdatingValue();
    }
    if (pendingUpdates.length) {
      setTimeout(Cell.invokeUpdates, 0);
    }
  },
  // this thing is called when there are no pending cell
  // recomputations. We use delay future value computations (XHR gets,
  // for example) and observers update till such 'quiescent'
  // state. Otherwise it's possible to initiate XHR GET only to abort
  // it few milliseconds later due to new dependency value
  completeGeneration: function () {
    var updatedCells = this.updatedCells;
    this.updatedCells = [];
    this.recalcGeneration = {};
    var i, len = updatedCells.length;
    for (i = 0; i < len; i++) {
      Cell.completeCellDelay(updatedCells[i]);
    }
  },
  completeCellDelay: function (cell) {
    var future = cell.pendingFuture;
    if (future && !future.started) {
      try {
        future.start(cell);
      } catch (e) {
        console.log("Got error trying to start future: ", e);
      }
    }
    if (cell.delayedBroadcast) {
      cell.delayedBroadcast.call(cell, cell.value);
      cell.delayedBroadcast = null
    }
  }
});

// Calls body with value (if not cell) or cell's value (via #getValue).
// body may be called now (if valueOrCell is not cell) or later when
// Cell's value is known (if it's not known yet)
//
// NOTE: that passing cells that may not ever obtain defined value is
// _dangerous_ and will leak.
Cell.resolveToValue = function (/*valueOrCell, otherValues... , body*/) {
  var args = _.toArray(arguments);
  var body = args[args.length-1];
  if (args.length < 2) {
    BUG();
  }
  // NOTE: we keep one count more in order to avoid unresolvedLeft
  // becoming 0 before final check is done. I.e. getValue calls body
  // right now if value is already defined. So we don't want body be
  // called two times. First from inside final getValue and second
  // time from final check
  var unresolvedLeft = args.length;
  for (var i = args.length-2; i >= 0; i--) {
    if (args[i] instanceof Cell) {
      (function (i) {
        this.getValue(function (aValue) {
          args[i] = aValue;
          unresolvedLeft--;
          if (unresolvedLeft === 0) {
            Cell.resolveToValue.apply(Cell, args);
          }
        });
      }).call(args[i], i);
    } else {
      unresolvedLeft--;
    }
  }
  if (--unresolvedLeft === 0) {
    // NOTE: body is still last arg. So to avoid issue with functions
    // that have optional last arg we need to pop body
    args.length--;
    body.apply(null, args);
  }
}

// returns function that resolves its arguments and then calls
// original function with same arguments. NOTE: see dangerous note
// about resolveToValue. NOTE: because waiting cells values involves
// waiting originalFunction may be called later and thus it's return
// value is ignored. Produced function always returns undefined
Cell.wrapWithArgsResolving = function (originalFunction) {
  return function () {
    var args = _.toArray(arguments);
    args.push(originalFunction);
    Cell.resolveToValue.apply(Cell, args);
  }
}

// returns new Cell that is result of applying function to given
// arguments, where Cells in arguments are resolved to their primitive
// values.
Cell.applyFunctionWithResolvedValues = function (originalFunction, self, args) {
  var resolvedArgsCell = Cell.compute(function (v) {
    return _.map(args, function (a) {
      return (a instanceof Cell) ? v.need(a) : a;
    });
  });
  return Cell.compute(function (v) {
    return originalFunction.apply(self, v.need(resolvedArgsCell));
  });
}

Cell.prototype.delegateInvalidationMethods = function (target) {
  var self = this;
  _.each(("recalculate recalculateAt recalculateAfterDelay invalidate").split(' '), function (methodName) {
    self[methodName] = $m(target, methodName);
  });
};

future.get = function (ajaxOptions, valueTransformer, nowValue, futureWrapper) {
  // var aborted = false;
  var options = {
    valueTransformer: valueTransformer,
    cancel: function () {
      // aborted = true;
      operation.cancel();
    },
    nowValue: nowValue
  };
  var operation;
  if (ajaxOptions.url === undefined) {
    throw new Error("url is undefined");
  }

  function initiateXHR(dataCallback) {
    var opts = {dataType: 'json',
                headers: {'Accept': 'application/json'},
                prepareReGet: function (opts) {
                  if (opts.errorMarker) {
                    // if we have error marker pass it to data
                    // callback now to mark error
                    if (!dataCallback.continuing(opts.errorMarker)) {
                      // if we were cancelled, cancel IOCenter
                      // operation
                      operation.cancel();
                    }
                  }
                },
                success: dataCallback};
    if (ajaxOptions.onError) {
      opts.error = function () {
        ajaxOptions.onError.apply(this, [dataCallback].concat(_.toArray(arguments)));
      };
    }
    operation = IOCenter.performGet(_.extend(opts, ajaxOptions));
  }
  return (futureWrapper || future)(initiateXHR, options);
};

future.withEarlyTransformer = function (earlyTransformer) {
  return {
    // we return a function
    get: function (ajaxOptions, valueTransformer, newValue, futureWrapper) {
      // that will delegate to future.get, but...
      return future.get(ajaxOptions, valueTransformer, newValue, futureReplacement);
      // will pass our function instead of future
      function futureReplacement(initXHR) {
        // that function will delegate to real future
        return (future || futureWrapper)(function (dataCallback) {
          // and once future is started
          // will call original future start function replacing dataCallback
          dataCallbackReplacement.cell = dataCallback.cell;
          dataCallbackReplacement.async = dataCallback.async;
          dataCallbackReplacement.continuing = dataCallback.continuing;
          initXHR(dataCallbackReplacement);
          function dataCallbackReplacement(value, status, xhr) {
            // replaced dataCallback will invoke earlyTransformer
            earlyTransformer(value, status, xhr);
            // and will finally pass data to real dataCallback
            dataCallback(value);
          }
        });
      }
    }
  };
}

future.pollingGET = function (ajaxOptions, valueTransformer, nowValue, futureWrapper) {
  var initiator = futureWrapper || future;
  var interval = 2000;
  if (ajaxOptions.interval) {
    interval = ajaxOptions.interval;
    delete ajaxOptions.interval;
  }
  return future.get(ajaxOptions, valueTransformer, nowValue, function (body, options) {
    return initiator(function (dataCallback) {
      var context = this;
      function dataCallbackWrapper(data) {
        if (!dataCallback.continuing(data)) {
          return;
        }
        setTimeout(function () {
          if (dataCallback.continuing(data)) {
            body.call(context, dataCallbackWrapper);
          }
        }, interval);
      }
      body.call(context, dataCallbackWrapper);
    }, options);
  });
};

future.infinite = function () {
  var rv = future(function () {
    rv.active = true;
    rv.cancel = function () {
      rv.active = false;
    };
  });
  return rv;
};

future.getPush = function (ajaxOptions, valueTransformer, nowValue, waitChange) {
  var options = {
    valueTransformer: valueTransformer,
    cancel: function () {
      operation.cancel();
    },
    nowValue: nowValue
  };
  var operation;
  var etag;

  if (!waitChange) {
    waitChange = 20000;
  }

  if (ajaxOptions.url === undefined) {
    throw new Error("url is undefined");
  }

  function sendRequest(dataCallback) {

    function gotData(data) {
      dataCallback.async.weak = false;

      etag = data && data.etag;
      // pass our data to cell
      if (dataCallback.continuing(data)) {
        // and submit new request if we are not cancelled
        _.defer(_.bind(sendRequest, null, dataCallback));
      }
    }

    var options = _.extend({dataType: 'json',
                            success: gotData},
                           ajaxOptions);

    if (options.url.indexOf("?") < 0) {
      options.url += '?waitChange=';
    } else {
      options.url += '&waitChange=';
    }
    options.url += waitChange;

    var originalUrl = options.url;
    if (etag) {
      options.url += "&etag=" + encodeURIComponent(etag);
      options.pushRequest = true;
      options.timeout = 30000;
    }
    options.prepareReGet = function (opt) {
      if (opt.errorMarker) {
        // if we have error marker pass it to data callback now to
        // mark error
        if (!dataCallback.continuing(opt.errorMarker)) {
          // if we were cancelled, cancel IOCenter operation
          operation.cancel();
          return;
        }
      }
      // make us weak so that cell invalidations will force new
      // network request
      dataCallback.async.weak = true;

      // recovering from error will 'undo' etag asking immediate
      // response, which we need in this case
      opt.url = originalUrl;
      return opt;
    };

    operation = IOCenter.performGet(options);
  }

  return future(sendRequest, options);
};

// this guy holds queue of failed actions and tries to repeat one of
// them every 5 seconds. Once it succeeds it changes status to healthy
// and repeats other actions
var ErrorQueue = mkClass({
  initialize: function () {
    var self = this;

    self.status = new Cell();
    self.status.setValue({
      healthy: true,
      repeating: false
    });
    self.repeatTimeout = null;
    self.queue = [];
    self.status.subscribeValue(function (s) {
      if (s.healthy) {
        self.repeatInterval = undefined;
      }
    });
  },
  repeatIntervalBase: 5000,
  repeatInterval: undefined,
  planRepeat: function () {
    var interval = this.repeatInterval;
    if (interval == null) {
      return this.repeatIntervalBase;
    }
    return Math.min(interval * 1.618, 300000);
  },
  submit: function (action) {
    this.queue.push(action);
    this.status.setValueAttr(false, 'healthy');
    if (this.repeatTimeout || this.status.value.repeating) {
      return;
    }

    var interval = this.repeatInterval = this.planRepeat();
    var at = (new Date()).getTime() + interval;
    this.status.setValueAttr(at, 'repeatAt');
    this.repeatTimeout = setTimeout($m(this, 'onTimeout'),
                                    interval);
  },
  forceAction: function (forcedAction) {
    var self = this;

    if (self.repeatTimeout) {
      clearTimeout(self.repeatTimeout);
      self.repeatTimeout = null;
    }

    if (!forcedAction) {
      if (!self.queue.length) {
        self.status.setValueAttr(true, 'healthy');
        return;
      }
      forcedAction = self.queue.pop();
    }

    self.status.setValueAttr(true, 'repeating');

    forcedAction(function (isOk) {
      self.status.setValueAttr(false, 'repeating');

      if (isOk == false) {
        return self.submit(forcedAction);
      }

      // if result is undefined or true, we issue next action
      if (isOk) {
        self.status.setValueAttr(true, 'healthy');
      }
      self.onTimeout();
    });
  },
  cancel: function (action) {
    this.queue = _.without(this.queue, action);
    if (this.queue.length || this.status.value.repeating) {
      return;
    }

    this.status.setValueAttr(true, 'healthy');
    if (this.repeatTimeout) {
      clearTimeout(this.repeatTimeout);
      this.repeatTimeout = null;
    }
  },
  onTimeout: function () {
    this.repeatTimeout = null;
    this.forceAction();
  }
});

// this is central place that defines policy of XHR error handling,
// all cells reads (there are no cells writes by definition) are
// tracked and controlled by this guy
var IOCenter = (function () {
  var ioCenterStatus = new Cell();
  ioCenterStatus.name("ioCenterStatus");
  ioCenterStatus.setValue({
    inFlightCount: 0
  });

  var errorQueue = new ErrorQueue();

  var statusCell = new Cell(function () {
    return _.extend({}, this.ioCenterStatus, this.errorQueueStatus);
  }, {
    ioCenterStatus: ioCenterStatus,
    errorQueueStatus: errorQueue.status
  });
  statusCell.name("IOCenter.status");
  statusCell.setValue(_.extend({}, ioCenterStatus.value, errorQueue.status.value));
  statusCell.invalidate();

  // S is short for self
  var S = {
    status: statusCell,
    errorQueue: errorQueue,
    forceRepeat: function () {
      errorQueue.forceAction();
    },
    performGet: function (options) {
      var usedOptions = _.clone(options);

      usedOptions.error = gotResponse;
      usedOptions.success = gotResponse;

      var op = {
        cancel: function () {
          if (op.xhr) {
            return Abortarium.abortRequest(op.xhr);
          }

          if (op.done) {
            return;
          }

          op.cancelled = true;

          // we're not done yet and we're not in-flight, so we must
          // be on error queue
          errorQueue.cancel(sendXHR);
        }
      };

      var extraCallback;

      // this is our function to send _and_ re-send
      // request. extraCallback parameter is used during re-sends to
      // execute some code when request is complete
      function sendXHR(_extraCallback) {
        extraCallback = _extraCallback;

        ioCenterStatus.setValueAttr(ioCenterStatus.value.inFlightCount + 1, 'inFlightCount');

        if (S.simulateDisconnect) {
          setTimeout(function () {
            usedOptions.error.call(usedOptions, {status: 500}, 'error');
          }, 200);
          return;
        } else if (S.pauseAjax) {
          return;
        }
        op.xhr = $.ajax(usedOptions);
      }
      sendXHR(function (isOk) {
        if (isOk != false) {
          return;
        }

        // our first time 'continuation' is if we've got error then we
        // submit us to repeat queue and maybe update options
        if (options.prepareReGet) {
          var newOptions = options.prepareReGet(usedOptions);
          if (newOptions != null)
            usedOptions = newOptions;
        }

        if (!op.cancelled) {
          errorQueue.submit(sendXHR);
        }
      });
      return op;

      function gotResponse(data, xhrStatus) {
        op.xhr = null;
        ioCenterStatus.setValueAttr(ioCenterStatus.value.inFlightCount - 1, 'inFlightCount');
        var args = arguments;
        var context = this;

        if (xhrStatus == 'success') {
          op.done = true;
          extraCallback(true);
          return options.success.apply(this, arguments);
        }

        var isOk = (function (xhr) {
          if (Abortarium.isAborted(xhr)) {
            return;
          }
          if (S.isNotFound(xhr)) {
            var missingValue = options.missingValue;
            if (!missingValue) {
              missingValue = options.missingValueProducer ? options.missingValueProducer(xhr, options) : undefined;
            }
            options.success.call(this,
                                 missingValue,
                                 'notfound', xhr);
            return;
          }

          // if our caller has it's own error handling strategy let him do it
          if (options.error) {
            options.error.apply(context, args);
            return;
          }

          return false;
        })(data);

        if (isOk != false) {
          op.done = true;
        }

        extraCallback(isOk);
      }
    },
    readStatus: function (xhr) {
      var status = 0;
      try {
        status = xhr.status
      } catch (e) {
        if (!xhr) {
          throw e;
        }
      }
      return status;
    },
    // we treat everything with 4xx status as not found because for
    // GET requests that we send, we don't have ways to re-send them
    // back in case of 4xx response comes.
    //
    // For example, we cannot do anything with sudden (and unexpected
    // anyway) "401-Authenticate" response, so we just interpret is as
    // if no resource exists.
    isNotFound: function (xhr) {
      var status = S.readStatus(xhr);
      return (400 <= status && status < 500);
    }
  };
  return S;
})();

IOCenter.staleness = new Cell(function (status) {
  return !status.healthy;
}, {
  status: IOCenter.status
});
IOCenter.staleness.name("IOCenter.staleness");

(function () {
  new Cell(function (status) {
    return status.healthy;
  }, {
    status: IOCenter.status
  }).subscribeValue(function (healthy) {
    console.log("IOCenter.status.healthy: ", healthy);
  });
})();

//This is new-style computed cells. Those have dynamic set of
//dependencies and have more lightweight, functional style. They are
//also lazy, which means that cell value is not (re)computed if
//nothing demands that value. Which happens when nothing is
//subscribed to this cell and it doesn't have dependent cells.
//
//Main guy here is Cell.compute. See it's comments. See also
//testCompute test in cells-test.js.
Cell.id = (function () {
  var counter = 1;
  return function (cell) {
    if (cell.__identity) {
      return cell.__identity;
    }
    return (cell.__identity = counter++);
  };
})();

FlexiFormulaCell = mkClass(Cell, {
  emptyFormula: function () {},
  initialize: function ($super, flexiFormula, isEager) {
    $super();

    var recalculate = $m(this, 'recalculate'),
        currentSources = this.currentSources = {};

    this.formula = function () {
      var rvPair = flexiFormula.call(this),
          newValue = rvPair[0],
          dependencies = rvPair[1];

      for (var i in currentSources) {
        if (dependencies[i]) {
          continue;
        } else {
          var pair = currentSources[i];
          pair[0].dependenciesSlot.unsubscribe(pair[1]);
          delete currentSources[i];
        }
      }

      for (var j in dependencies) {
        if (j in currentSources) {
          continue;
        } else {
          var cell = dependencies[j],
              slave = cell.dependenciesSlot.subscribeWithSlave(recalculate);

          currentSources[j] = [cell, slave];
        }
      }

      return newValue;
    };

    this.formulaContext = {self: this};
    if (isEager) {
      this.effectiveFormula = this.formula;
      this.recalculate();
    } else {
      this.effectiveFormula = this.emptyFormula;
      this.setupDemandObserving();
    }
  },
  setupDemandObserving: function () {
    var demand = {},
        self = this;
    _.each(
      {
        changed: self.changedSlot,
        'undefined': self.undefinedSlot,
        dependencies: self.dependenciesSlot
      },
      function (slot, name) {
        slot.__demandChanged = function (newDemand) {
          demand[name] = newDemand;
          react();
        };
      }
    );
    function react() {
      var haveDemand = demand.dependencies || demand.changed || demand['undefined'];

      if (!haveDemand) {
        self.detach();
      } else {
        self.attachBack();
      }
    }
  },
  needsRefresh: function (newValue) {
    if (this.value === undefined) {
      return true;
    }
    if (newValue instanceof Future) {
      // we don't want to recalculate values that involve futures
      return false;
    }
    return this.isValuesDiffer(this.value, newValue);
  },
  attachBack: function () {
    if (this.effectiveFormula === this.formula) {
      return;
    }

    this.effectiveFormula = this.formula;

    // This piece of code makes sure that we don't refetch things that
    // are already fetched
    var needRecalculation;
    if (this.hadPendingFuture) {
      // if we had future in progress then we will start it back
      this.hadPendingFuture = false;
      needRecalculation = true;
    } else {
      // NOTE: this has side-effect of updating formula dependencies and
      // subscribing to them back
      var newValue = this.effectiveFormula.call(this.mkFormulaContext());
      needRecalculation = this.needsRefresh(newValue);
    }

    if (needRecalculation) {
      // but in the end we call recalculate to correctly handle
      // everything. Calling it directly might start futures which
      // we don't want. And copying it's code here is even worse.
      //
      // Use of prototype is to avoid issue with overriden
      // recalculate. See Cell#delegateInvalidationMethods. We want
      // exact original recalculate behavior here.
      Cell.prototype.recalculate.call(this);
    }
  },
  detach: function () {
    var currentSources = this.currentSources;
    for (var id in currentSources) {
      if (id) {
        var pair = currentSources[id];
        pair[0].dependenciesSlot.unsubscribe(pair[1]);
        delete currentSources[id];
      }
    }
    if (this.pendingFuture) {
      this.hadPendingFuture = true;
    }
    this.effectiveFormula = this.emptyFormula;
    this.setValue(this.value);  // this cancels any in-progress
                                // futures
    clearTimeout(this.recalculateAtTimeout);
    this.recalculateAtTimeout = undefined;
  },
  setSources: function () {
    throw new Error("unsupported!");
  },
  mkFormulaContext: function () {
    return this.formulaContext;
  },
  getSourceCells: function () {
    var rv = [],
        sources = this.currentSources;
    for (var id in sources) {
      if (id) {
        rv.push(sources[id][0]);
      }
    }
    return rv;
  }
});

FlexiFormulaCell.noValueMarker = (function () {
  try {
    throw {};
  } catch (e) {
    return e;
  }
})();

FlexiFormulaCell.makeComputeFormula = function (formula) {
  var dependencies,
      noValue = FlexiFormulaCell.noValueMarker;

  function getValue(cell) {
    var id = Cell.id(cell);
    if (!dependencies[id]) {
      dependencies[id] = cell;
    }
    return cell.value;
  }

  function need(cell) {
    var v = getValue(cell);
    if (v === undefined) {
      throw noValue;
    }
    return v;
  }

  getValue.need = need;

  return function () {
    dependencies = {};
    var newValue;
    try {
      newValue = formula.call(this, getValue);
    } catch (e) {
      if (e === noValue) {
        newValue = undefined;
      } else {
        throw e;
      }
    }
    var deps = dependencies;
    dependencies = null;
    return [newValue, deps];
  };
};

// Creates cell that is computed by running formula. This function is
// passed V argument. Which is a function that gets values of other
// cells. It is necessary to obtain dependent cell values via that
// function, so that all dependencies are recorded. Then if any of
// (dynamic) dependencies change formula is recomputed. Which may
// produce (apart from new value) new set of dependencies.
//
// V also has a useful helper: V.need which is just as V extracts
// values from cells. But it differs from V in that undefined values
// are never returned. Special exception is raised instead to signal
// that formula value is undefined.
Cell.compute = function (formula) {
  var _FlexiFormulaCell = arguments[1] || FlexiFormulaCell,
      f = FlexiFormulaCell.makeComputeFormula(formula);

  return new _FlexiFormulaCell(f);
};

Cell.computeEager = function (formula) {
  var _FlexiFormulaCell = arguments[1] || FlexiFormulaCell,
      f = FlexiFormulaCell.makeComputeFormula(formula);

  return new _FlexiFormulaCell(f, true);
};

(function () {
  var constructor = function (dependencies) {
    this.dependencies = dependencies;
  }

  function def(delegateMethodName) {
    function delegatedMethod(body) {
      var dependencies = this.dependencies;

      return Cell[delegateMethodName](function (v) {
        var args = [v];
        args.length = dependencies.length+1;
        var i = dependencies.length;
        while (i--) {
          var value = v(dependencies[i]);
          if (value === undefined) {
            return;
          }
          args[i+1] = value;
        }
        return body.apply(this, args);
      });
    }
    constructor.prototype[delegateMethodName] = delegatedMethod;
  }

  def('compute');
  def('computeEager');

  Cell.needing = function () {
    return new constructor(arguments);
  };
})();

// subscribes to multiple cells as a single subscription
Cell.subscribeMultipleValues = function (body/*, cells... */) {
  var args = _.rest(arguments);
  var badArgs = _.reject(args, function (a) {return a instanceof Cell});
  if (badArgs.length) {
    console.log("has non cell args in subscribeMultipleValues: ", badArgs);
    throw new Error("bad args to subscribeMultipleValues");
  }
  return Cell.compute(function (v) {
    return _.map(args, v);
  }).subscribeValue(function (arr) {
    if (arr === undefined) {
      return;
    }
    body.apply(null, arr);
  });
}

// invoke body when all cell values are stable. I.e. there's no pending recomputations
Cell.waitQuiescence = function (body) {
  Cell.compute(function () {return this}).getValue(function () {
    body();
  });
}


function ensureElementId(jq) {
  jq.each(function () {
    if (this.id)
      return;
    this.id = _.uniqueId('gen');
  });
  return jq;
}

// this cell type supports only string values and synchronizes
// window.location hash fragment value and cell value
var StringHashFragmentCell = mkClass(Cell, {
  initialize: function ($super, paramName) {
    $super();
    this.paramName = paramName;
    watchHashParamChange(this.paramName, $m(this, 'interpretHashFragment'));
    this.subscribeAny($m(this, 'updateHashFragment'));
  },
  interpretHashFragment: function (value) {
    this.setValue(value);
  },
  updateHashFragment: function () {
    setHashFragmentParam(this.paramName, this.value);
  }
});

// this cell synchronizes set of values (not necessarily strings) and
// set of string values of window.location hash fragment
var HashFragmentCell = mkClass(Cell, {
  initialize: function ($super, paramName, options) {
    $super();

    this.paramName = paramName;
    this.options = _.extend({
      firstValueIsDefault: false
    }, options || {});

    this.idToItems = {};
    this.items = [];
    this.defaultId = undefined;
    this.selectedId = undefined;
  },
  interpretState: function (id) {
    var item = this.idToItems[id];
    if (!item)
      return;

    this.setValue(item.value);
  },
  beforeChangeHook: function (value) {
    if (value === undefined) {
      setHashFragmentParam(this.paramName, undefined);

      this.selectedId = undefined;
      return value;
    }

    var pickedItem = _.detect(this.items, function (item) {
      return item.value == value;
    });
    if (!pickedItem)
      throw new Error("Aiiyee. Unknown value: " + value);

    this.selectedId = pickedItem.id;
    this.pushState(this.selectedId);

    return pickedItem.value;
  },
  // setValue: function (value) {
  //   var _super = $m(this, 'setValue', Cell);
  //   console.log('calling setValue: ', value, getBacktrace());
  //   return _super(value);
  // },
  pushState: function (id) {
    id = String(id);
    var currentState = getHashFragmentParam(this.paramName);
    if (currentState == id || (currentState === undefined && id == this.defaultId))
      return;
    setHashFragmentParam(this.paramName, id);
  },
  addItem: function (id, value, isDefault) {
    var item = {id: id, value: value, index: this.items.length};
    this.items.push(item);
    if (isDefault || (item.index == 0 && this.options.firstItemIsDefault))
      this.defaultId = id;
    this.idToItems[id] = item;
    return this;
  },
  finalizeBuilding: function () {
    watchHashParamChange(this.paramName, this.defaultId, $m(this, 'interpretState'));
    this.interpretState(getHashFragmentParam(this.paramName));
    return this;
  }
});

var BaseClickSwitchCell = mkClass(HashFragmentCell, {
  initialize: function ($super, paramName, options) {
    options = _.extend({
      selectedClass: 'selected',
      linkSelector: '*',
      eventSpec: 'click',
      bindMethod: 'live'
    }, options);

    $super(paramName, options);

    this.subscribeAny($m(this, 'updateSelected'));

    var self = this;
    $(self.options.linkSelector)[self.options.bindMethod](self.options.eventSpec, function (event) {
      self.eventHandler(this, event);
    })
  },
  updateSelected: function () {
    var value = this.value;
    if (value == undefined) {
      return;
    }

    var index = _.indexOf(_(this.items).pluck('value'), value);
    if (index < 0) {
      throw new Error('invalid value!');
    }

    var getSelector = _.bind(this.getSelector, this);
    var selectors = _.map(this.idToItems,
      function(value, key) {
        return getSelector(key);
      }).join(',');
    $(selectors).removeClass(this.options.selectedClass);

    var id = this.items[index].id;
    $(getSelector(id)).addClass(this.options.selectedClass);
  },
  eventHandler: function (element, event) {
    var id = this.extractElementID(element);
    var item = this.idToItems[id];
    if (!item)
      return;

    this.pushState(id);
    event.preventDefault();
  }
});

// this cell type associates a set of HTML links with a set of values
// (any type) and persists selected value in window.location hash
// fragment
var LinkSwitchCell = mkClass(BaseClickSwitchCell, {
  getSelector: function (id) {
    return '#' + id;
  },
  extractElementID: function (element) {
    return element.id;
  },
  addLink: function (link, value, isDefault) {
    if (link.size() == 0)
      throw new Error('missing link for selector: ' + link.selector);
    var id = ensureElementId(link).attr('id');
    this.addItem(id, value, isDefault);
    return this;
  }
});

// This cell type associates a set of CSS classes with a set of
// values and persists selected value in window.location hash
// fragment. Clicking on any element with on of configured classes
// switches this cell to value associated with that class.
//
// All CSS classes in set must have prefix paramName
var LinkClassSwitchCell = mkClass(BaseClickSwitchCell, {
  getSelector: function (id) {
    return '.' + id;
  },
  extractElementID: function (element) {
    var classNames = element.className.split(' ');
    for (var i = classNames.length-1; i >= 0; i--) {
      var v = classNames[i];
      if (this.idToItems[v])
        return v;
    }
  }
});

var TabsCell = mkClass(HashFragmentCell, {
  initialize: function ($super, paramName, tabsSelector, panesSelector, values, options) {
    var self = this;
    $super(paramName, $.extend({firstItemIsDefault: true},
                               options || {}));

    self.tabsSelector = tabsSelector;
    self.panesSelector = panesSelector;
    var tabsOptions = $.extend({},
                               options || {current: 'selected'},
                               {api: true});
    self.api = $(tabsSelector).tabs(panesSelector, tabsOptions);

    self.api.onBeforeClick($m(this, 'onTabClick'));
    self.subscribeAny($m(this, 'updateSelected'));

    _.each(values, function (val, index) {
      self.addItem(index, val);
    });
    self.finalizeBuilding();
  },
  onTabClick: function (event, index) {
    var item = this.idToItems[index];
    if (!item)
      return;
    this.pushState(index);
  },
  updateSelected: function () {
    this.api.click(Number(this.selectedId));
  }
});


var StringSetHashFragmentCell = mkClass(StringHashFragmentCell, {
  initialize: function ($super, paramName) {
    $super.apply(null, _.rest(arguments));
    // TODO: setValue([]) may trigger interesting cells bug with setValue
    // being called from cell watcher. That has side effect of wiping
    // slaves list
    this.value = [];
  },
  interpretHashFragment: function (value) {
    if (value == null || value == "")
      value = [];
    else
      value = value.split(",");

    this.setValue(value);
  },
  updateHashFragment: function () {
    var value = this.value;
    if (value == null || value.length == 0) {
      value = null;
    } else
      value = value.concat([]).sort().join(',');
    setHashFragmentParam(this.paramName, value);
  },
  addValue: function (value) {
    return this.modifyValue(function (set) {
      return _.uniq(set.concat([value]))
    });
  },
  removeValue: function (value) {
    return this.modifyValue(function (set) {
      return _.without(set, value);
    });
  },
  reset: function () {
    this.setValue([]);
  }
});