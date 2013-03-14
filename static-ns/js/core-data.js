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
function addBasicAuth(xhr, login, password) {
  var auth = 'Basic ' + Base64.encode(login + ':' + password);
  xhr.setRequestHeader('Authorization', auth);
}

function onUnexpectedXHRError(xhr, xhrStatus, errMsg) {
  var status;
  var readyState;
  var self = this;

  window.onUnexpectedXHRError = function () {};

  if (Abortarium.isAborted(xhr)) {
    return;
  }

  // for manual interception
  if ('debuggerHook' in onUnexpectedXHRError) {
    onUnexpectedXHRError.debuggerHook(xhr, xhrStatus, errMsg);
  }

  try {
    status = xhr.status;
  } catch (e) {}

  try {
    readyState = xhr.readyState;
  } catch (e) {}

  if (status === 401) {
    $.cookie('auth', null);
    return reloadApp();
  }

  if ('JSON' in window && 'sessionStorage' in window) {
    (function () {
      var json;
      var responseText;
      var s = {};
      var e;
      var er;

      try {
        responseText = String(xhr.responseText);
      } catch (e) {}

      try {
        _.each(self, function (value, key) {
          if (_.isString(key) || _.isNumber(key)) {
            s[key] = value;
          }
        });
        json = JSON.stringify(s);
      } catch (er) {
        json = "";
      }
      sessionStorage.reloadCause = "s: " + json +
        "\nxhrStatus: " + xhrStatus + ",\nxhrReadyState: " + readyState +
        ",\nerrMsg: " + errMsg + ",\nstatusCode: " + status +
        ",\nresponseText:\n" + responseText;
      sessionStorage.reloadTStamp = (new Date()).valueOf();
    }());
  }

  var reloadInfo = $.cookie('ri');
  var ts;

  var now = (new Date()).valueOf();
  if (reloadInfo) {
    ts = parseInt(reloadInfo, 10);
    if ((now - ts) < 15*1000) {
      $.cookie('rf', null); // clear reload-info cookie, so that
                            // manual reload don't cause 'console has
                            // been reloaded' flash message

      var details = DAL.cells.currentPoolDetailsCell.value;
      var notAlone = details && details.nodes.length > 1;
      var msg = 'The application received multiple invalid responses from the server.  The server log may have details on this error.  Reloading the application has been suppressed.';
      if (notAlone) {
        msg += '\n\nYou may be able to load the console from another server in the cluster.';
      }
      alert(msg);

      return;
    }
  }

  $.cookie('ri', String((new Date()).valueOf()), {expires:0});
  $.cookie('rf', '1');
  reloadAppWithDelay(500);
}

$.ajaxSetup({
  error: onUnexpectedXHRError,
  timeout: 30000,
  cache: false,
  beforeSend: function (xhr, options) {
    if (DAL.login) {
      addBasicAuth(xhr, DAL.login, DAL.password);
    }
    xhr.setRequestHeader('invalid-auth-response', 'on');
    xhr.setRequestHeader('Cache-Control', 'no-cache');
    xhr.setRequestHeader('Pragma', 'no-cache');
  },
  dataFilter: function (data, type) {
    if (type === "json" && data == "") {
      throw new Error("empty json");
    }
    return data;
  }
});

var DAL = {
  ready: false,
  version: undefined,
  cells: {},
  onReady: function (thunk) {
    if (DAL.ready) {
      thunk.call(null);
    } else {
      $(window).one('dao:ready', function () {thunk();});
    }
  },
  setAuthCookie: function (user, password) {
    if (user != '') {
      var auth = Base64.encode([user, ':', password].join(''));
      $.cookie('auth', auth);
    } else {
      $.cookie('auth', null);
    }
  },
  appendedVersion: false,
  parseVersion: function (str) { // Example: "1.8.0r-9-ga083a1e-enterprise"
    var a = str.split(/[-_]/);
    a[0] = (a[0].match(/[0-9]+\.[0-9]+\.[0-9]+/) || ["0.0.0"])[0]
    a[1] = a[1] || "0"
    a[2] = a[2] || "unknown"
    a[3] = a[3] || "DEV"
    return a; // Example result: ["1.8.0", "9", "ga083a1e", "enterprise"]
  },
  prettyVersion: function(str, full) {
    var a = DAL.parseVersion(str);
    // Example default result: "1.8.0 enterprise edition (build-7)"
    // Example full result: "1.8.0 enterprise edition (build-7-g35c9cdd)"
    var suffix = "";
    if (full) {
      suffix = '-' + a[2];
    }
    return [a[0], a[3], "edition", "(build-" + a[1] + suffix + ")"].join(' ');
  },
  loginSuccess: function (data) {
    var rows = data.pools;

    var implementationVersion = data.implementationVersion;

    (function () {
      var match = /(\?|&)forceVersion=([^&]+)/.exec(document.location.href);
      if (!match) {
        return;
      }
      implementationVersion = decodeURIComponent(match[2]);
      console.log("forced version: ", implementationVersion);
    })();

    if (implementationVersion) {
      DAL.version = implementationVersion;
      DAL.componentsVersion = data.componentsVersion;
      DAL.uuid = data.uuid;
      var parsedVersion = DAL.parseVersion(implementationVersion);
      DAL.isEnterprise = (parsedVersion[3] === 'enterprise');
      if (!DAL.appendedVersion) {
        document.title = document.title +
          " (" + parsedVersion[0] + ")";
        var v = DAL.prettyVersion(implementationVersion);
        $('.version > .couchbase-version').text(v).parent().show();
        DAL.appendedVersion = true;
      }
    }

    var provisioned = !!rows.length;
    var authenticated = data.isAdminCreds;
    if (provisioned && !authenticated) {
      return false;
    }

    if (provisioned && authenticated && !DAL.login) {
      alert("WARNING: Your browser has cached administrator Basic HTTP authentication credentials. You need to close and re-open it to clear that cache.");
    }

    DAL.ready = true;
    $(window).trigger('dao:ready');

    DAL.cells.poolList.setValue(rows);
    DAL.setAuthCookie(DAL.login, DAL.password);

    $('#secure_server_buttons').attr('class', DAL.login ? 'secure_disabled' : 'secure_enabled');


    // If the cluster appears to be configured, then don't let user go
    // back through init dialog.
    SetupWizard.show(provisioned ? 'done' : '');

    return true;
  },
  switchSection: function (section) {
    DAL.switchedSection = section;
    if (DAL.sectionsEnabled) {
      DAL.cells.mode.setValue(section);
    }
  },
  enableSections: function () {
    DAL.sectionsEnabled = true;
    DAL.cells.mode.setValue(DAL.switchedSection);
  },
  tryNoAuthLogin: function () {
    var rv;
    var auth;
    var arr;

    function cb(data, status) {
      if (status === 'success') {
        rv = DAL.loginSuccess(data);
      }
    }

    $.ajax({
      type: 'GET',
      url: "/pools",
      dataType: 'json',
      async: false,
      success: cb,
      error: cb});

    if (!rv && (auth = $.cookie('auth'))) {
      arr = Base64.decode(auth).split(':');
      DAL.login = arr[0];
      DAL.password = arr[1];

      $('#auth_dialog [name=login]').val(arr[0]);

      $.ajax({
        type: 'GET',
        url: "/pools",
        dataType: 'json',
        async: false,
        success: cb,
        error: cb});
    }

    return rv;
  },
  performLogin: function (login, password, callback) {
    this.login = login;
    this.password = password;

    function cb(data, status) {
      if (status === 'success') {
        if (!DAL.loginSuccess(data)) {
          status = 'error';
        }
      }
      if (callback) {
        callback(status);
      }
    }

    $.ajax({
      type: 'GET',
      url: "/pools",
      dataType: 'json',
      success: cb,
      error: cb});
  }
};


(function () {
  this.mode = new Cell();
  this.poolList = new Cell();

  // this cell lowers comet/push timeout for overview sections _and_ makes
  // sure we don't fetch pool details if mode is not set (we're in
  // wizard)
  var poolDetailsPushTimeoutCell = new Cell(function (mode) {
    if (mode === 'overview' || mode === 'manage_servers') {
      return 3000;
    }
    return 20000;
  }, {
    mode: this.mode
  });

  this.currentPoolDetailsCell = Cell.needing(this.poolList,
                                             poolDetailsPushTimeoutCell)
    .computeEager(function (v, poolList, pushTimeout) {
      var url;

      if (!poolList[0]) {
        return;
      }

      url = poolList[0].uri;
      function poolDetailsValueTransformer(data) {
        // we clear pool's name to display empty name in analytics
        data.name = '';
        return data;
      }

      return future.getPush({url: url,
                             // NOTE: when this request gets 404 it
                             // means cluster was re-initialized, so
                             // we should reload app into setup wizard
                             missingValueProducer: _.bind(reloadApp, window, undefined)},
                            poolDetailsValueTransformer,
                            this.self.value, pushTimeout);
    });
  this.currentPoolDetailsCell.equality = _.isEqual;
  this.currentPoolDetailsCell.name("currentPoolDetailsCell");

  this.nodeStatusesURICell = Cell.computeEager(function (v) {
    return v.need(DAL.cells.currentPoolDetailsCell).nodeStatusesUri;
  });

  this.nodeStatusesCell = Cell.compute(function (v) {
    return future.get({url: v.need(DAL.cells.nodeStatusesURICell)});
  });

}).call(DAL.cells);

(function () {
  var hostnameComparator = mkComparatorByProp('hostname', naturalSort);
  var pendingEject = []; // nodes to eject on next rebalance
  var pending = []; // nodes for pending tab
  var active = []; // nodes for active tab
  var allNodes = []; // all known nodes
  var cell;

  function formula(details, detailsAreStale) {
    var self = this;

    var pending = [];
    var active = [];
    allNodes = [];

    var nodes = details.nodes;
    var nodeNames = _.pluck(nodes, 'hostname');
    _.each(nodes, function (n) {
      var mship = n.clusterMembership;
      if (mship === 'active') {
        active.push(n);
      } else {
        pending.push(n);
      }
      if (mship === 'inactiveFailed') {
        active.push(n);
      }
    });

    var stillActualEject = [];
    _.each(pendingEject, function (node) {
      var original = _.detect(nodes, function (n) {
        return n.otpNode == node.otpNode;
      });
      if (!original || original.clusterMembership === 'inactiveAdded') {
        return;
      }
      stillActualEject.push(original);
      original.pendingEject = true;
    });

    pendingEject = stillActualEject;

    pending = pending = pending.concat(pendingEject);
    pending.sort(hostnameComparator);
    active.sort(hostnameComparator);

    allNodes = _.uniq(active.concat(pending));

    var reallyActive = _.select(active, function (n) {
      return n.clusterMembership === 'active' && !n.pendingEject && n.status !== 'unhealthy';
    });

    if (reallyActive.length == 1) {
      reallyActive[0].lastActive = true;
    }

    _.each(allNodes, function (n) {
      var interestingStats = n.interestingStats;
      if (interestingStats && ('couch_docs_data_size' in interestingStats)) {
        n.couchDataSize = interestingStats.couch_docs_data_size + interestingStats.couch_views_data_size;
        n.couchDiskUsage = interestingStats.couch_docs_actual_disk_size + interestingStats.couch_views_actual_disk_size;
      }

      n.ejectPossible = !detailsAreStale && !n.pendingEject;
      n.failoverPossible = !detailsAreStale && (n.clusterMembership !== 'inactiveFailed');
      n.reAddPossible = !detailsAreStale && (n.clusterMembership === 'inactiveFailed' && n.status !== 'unhealthy');

      var nodeClass = '';
      if (n.clusterMembership === 'inactiveFailed') {
        nodeClass = n.status === 'unhealthy' ? 'server_down' : 'failed_over';
      } else if (n.status === 'healthy') {
        nodeClass = 'server_up';
      } else if (n.status === 'unhealthy') {
        nodeClass = 'server_down';
      } else if (n.status === 'warmup') {
        nodeClass = 'server_warmup';
      }
      if (n.lastActive) {
        nodeClass += ' last-active';
      }
      n.nodeClass = nodeClass;
    });

    return {
      stale: detailsAreStale,
      pendingEject: pendingEject,
      pending: pending,
      active: active,
      allNodes: allNodes
    };
  }

  cell = DAL.cells.serversCell = new Cell(formula, {
    details: DAL.cells.currentPoolDetailsCell,
    detailsAreStale: IOCenter.staleness
  });

  cell.cancelPendingEject = function (node) {
    node.pendingEject = false;
    pendingEject = _.without(pendingEject, node);
    cell.invalidate();
  };
}());

// detailedBuckets
(function (cells) {
  var currentPoolDetailsCell = cells.currentPoolDetailsCell;

  // we're using separate 'intermediate' cell to isolate all updates
  // of currentPoolDetailsCell from updates of buckets uri (which
  // basically never happens)
  var bucketsURI = DAL.cells.bucketsURI = Cell.compute(function (v) {
    return v.need(currentPoolDetailsCell).buckets.uri;
  });

  var rawDetailedBuckets = Cell.compute(function (v) {
    return future.get({url: v.need(bucketsURI)});
  });
  rawDetailedBuckets.keepValueDuringAsync = true;

  // we use few attrs of pool details for massaging buckets list,
  // extract them so that we don't re-massage buckets list when
  // irrelevant poolDetails attributes change value.
  //
  // 'null' indicates invalid pool details
  var massagedUsedPoolDetails = Cell.compute(function (v) {
    var poolDetails = v.need(currentPoolDetailsCell);
    var storageTotals = poolDetails.storageTotals;
    if (!storageTotals || !storageTotals.ram) {
      // this might happen if ns_doctor is down, which often happens
      // after failover
      return null;
    }
    return {storageTotals: storageTotals,
            serversCount: poolDetails.nodes.length};
  });
  massagedUsedPoolDetails.equality = _.isEqual;

  // force refetch of pool details if there is still no storageTotals for 2 seconds
  (function () {
    var timeoutId;
    massagedUsedPoolDetails.subscribeValue(function (val) {
      if (val === null && timeoutId === undefined) {
        timeoutId = setTimeout(function () {
          timeoutId = undefined;
          currentPoolDetailsCell.recalculate();
        }, 2000);
      }
    });
  })();

  var nonNullMassagedDetails = Cell.compute(function (v) {
    var rv = v(massagedUsedPoolDetails);
    if (rv == null)
      return;
    return rv;
  });

  cells.bucketsListCell = Cell.compute(function (v) {
    var values = v.need(rawDetailedBuckets);
    var massagedDetails = v.need(nonNullMassagedDetails);

    values = _.clone(values);
    // adding a child object for storing bucket by their type
    values.byType = {"membase":[], "memcached":[]};

    var storageTotals = massagedDetails.storageTotals;

    _.each(values, function (bucket) {
      if (bucket.bucketType == 'memcached') {
        bucket.bucketTypeName = 'Memcached';
      } else if (bucket.bucketType == 'membase') {
        bucket.bucketTypeName = 'Couchbase';
      } else {
        bucket.bucketTypeName = bucket.bucketType;
      }
      if (values.byType[bucket.bucketType] === undefined) {
        values.byType[bucket.bucketType] = [];
      }
      values.byType[bucket.bucketType].push(bucket);

      bucket.ramQuota = bucket.quota.ram;
      bucket.totalRAMSize = storageTotals.ram.total;
      bucket.totalRAMUsed = bucket.basicStats.memUsed;
      bucket.otherRAMSize = storageTotals.ram.used - bucket.totalRAMUsed;
      bucket.totalRAMFree = storageTotals.ram.total - storageTotals.ram.used;

      bucket.RAMUsedPercent = calculatePercent(bucket.totalRAMUsed, bucket.totalRAMSize);
      bucket.RAMOtherPercent = calculatePercent(bucket.totalRAMUsed + bucket.otherRAMSize, bucket.totalRAMSize);

      bucket.totalDiskSize = storageTotals.hdd.total;
      bucket.totalDiskUsed = bucket.basicStats.diskUsed;
      bucket.otherDiskSize = storageTotals.hdd.used - bucket.totalDiskUsed;
      bucket.totalDiskFree = storageTotals.hdd.total - storageTotals.hdd.used;

      bucket.diskUsedPercent = calculatePercent(bucket.totalDiskUsed, bucket.totalDiskSize);
      bucket.diskOtherPercent = calculatePercent(bucket.otherDiskSize + bucket.totalDiskUsed, bucket.totalDiskSize);
      var h = _.reduce(_.pluck(bucket.nodes, 'status'),
                       function(counts, stat) {
                         counts[stat] = (counts[stat] || 0) + 1;
                         return counts;
                       },
                       {});
      // order of these values is important to match pie chart colors
      bucket.healthStats = [h.healthy || 0, h.warmup || 0, h.unhealthy || 0];
    });

    return values;
  });
  cells.bucketsListCell.equality = _.isEqual;
  cells.bucketsListCell.delegateInvalidationMethods(rawDetailedBuckets);

  cells.bucketsListCell.refresh = function (callback) {
    var cell = cells.bucketsListCell;
    if (callback) {
      cell.changedSlot.subscribeOnce(callback);
    }
    cell.invalidate();
  };
})(DAL.cells);

(function () {
  var capiBaseCell = DAL.cells.capiBase = Cell.computeEager(function (v) {
    var details = v(DAL.cells.currentPoolDetailsCell);
    if (!details) {
      // if we already have value, but pool details are undefined
      // (likely reloading), keep old value to avoid interfering with
      // in-flight CAPI requests
      return this.self.value;
    }

    var nodes = details.nodes;
    var thisNode = _.detect(nodes, function (n) {return n.thisNode;});
    if (!thisNode) {
      return this.self.value;
    }

    return thisNode.couchApiBase;
  }).name("capiBaseCell");

  DAL.cells.runningInCompatMode = Cell.computeEager(function (v) {
    var details = v(DAL.cells.currentPoolDetailsCell);
    if (!details) {
      // when our dependent cells is unknown we keep our old value
      return this.self.value;
    }
    return !v(capiBaseCell);
  });

  $.ajaxPrefilter(function (options, originalOptions, jqXHR) {
    var capiBase = capiBaseCell.value;
    if (!capiBase) {
      return;
    }
    var capiBaseLen = capiBase.length;

    if (options.crossDomain && options.url.substring(0, capiBaseLen) === capiBase) {
      options.crossDomain = false;
      options.url = "/couchBase/" + options.url.slice(capiBaseLen);
    }
  });

  DAL.subscribeWhenSection = function (cell, section, body) {
    var intermediary = Cell.compute(function (v) {
      if (v.need(DAL.cells.mode) !== section)
        return;
      return v(cell);
    });
    return intermediary.subscribeValue(body);
  };
})();

(function () {
  var tasksProgressURI = DAL.cells.tasksProgressURI = Cell.compute(function (v) {
    return v.need(DAL.cells.currentPoolDetailsCell).tasks.uri;
  }).name("tasksProgressURI");
  var tasksProgressCell = DAL.cells.tasksProgressCell = Cell.computeEager(function (v) {
    var uri = v.need(tasksProgressURI);
    return future.get({url: uri});
  }).name("tasksProgressCell");
  tasksProgressCell.keepValueDuringAsync = true;
  var tasksRefreshPeriod = DAL.cells.tasksRefreshPeriod = Cell.compute(function (v) {
    var tasks = v.need(tasksProgressCell);
    var minPeriod = 1 << 28;
    _.each(tasks, function (taskInfo) {
      var period = taskInfo.recommendedRefreshPeriod;
      if (!period) {
        return;
      }
      period = (period * 1000) >> 0;
      if (period < minPeriod) {
        minPeriod = period;
      }
    });
    return minPeriod;
  }).name("tasksRefreshPeriod");
  Cell.subscribeMultipleValues(function (period) {
    if (!period) {
      return;
    }
    tasksProgressCell.recalculateAfterDelay(period);
  }, tasksRefreshPeriod, tasksProgressCell);
})();

var RecentlyCompacted = mkClass.turnIntoLazySingleton(mkClass({
  initialize: function () {
    setInterval(_.bind(this.onInterval, this), 2000);
    this.triggeredCompactions = {};
    this.startedCompactionsCell = (new Cell()).name("startedCompactionsCell");
    this.updateCell();
  },
  updateCell: function () {
    var obj = {};
    _.each(this.triggeredCompactions, function (v, k) {
      obj[k] = true;
    });
    this.startedCompactionsCell.setValue(obj);
  },
  registerAsTriggered: function (url, undoBody, element) {
    if (this.triggeredCompactions[url]) {
      this.gcThings();
      if (this.triggeredCompactions[url]) {
        return;
      }
    }
    var desc = {url: url,
                undoBody: undoBody || Function(),
                gcAt: (new Date()).valueOf() + 10000};
    this.triggeredCompactions[url] = desc;
    this.updateCell();
  },
  canCompact: function (url) {
    var desc = this.triggeredCompactions[url];
    if (!desc) {
      return true;
    }
    return desc.gcAt <= (new Date()).valueOf();
  },
  gcThings: function () {
    var now = new Date();
    var expired = [];
    var notExpired = {};
    _.each(this.triggeredCompactions, function (desc) {
      if (desc.gcAt > now) {
        notExpired[desc.url] = desc;
      } else {
        expired.push(desc);
      }
    });
    this.triggeredCompactions = notExpired;
    _.each(expired, function (desc) {
      var undo = desc.undoBody;
      if (undo) {
        undo();
      }
    });
    this.updateCell();
  },
  onInterval: function () {
    this.gcThings();
  }
}));
