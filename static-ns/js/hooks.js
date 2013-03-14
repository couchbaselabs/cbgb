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
var TestingSupervisor = {
  chooseSingle: function (arg, predicate) {
    if (!_.isArray(arg)) {
      var key = this.chooseSingle(_.keys(arg), predicate);
      return arg[key];
    }
    var passing = _.select(arg, function (id) {
      return predicate(id);
    });
    if (passing.length != 1)
      throw new Error("Invalid number of predicate-passing of items: " + passing.length);
    return passing[0];
  },
  chooseVisible: function (arg) {
    return this.chooseSingle(arg, function (id) {
      return $($i(id)).css('display') != 'none';
    });
  },
  chooseSelected: function (arg) {
    return this.chooseSingle(arg, function (id) {
      return $($i(id)).hasClass('selected');
    });
  },
  activeSection: function () {
    return this.chooseVisible(['overview', 'alerts', 'settings']);
  },
  activeGraphZoom: function () {
    return this.chooseSelected({
      'overview_graph_zoom_real_time': 'real_time',
      'overview_graph_zoom_one_hr' : 'one_hr',
      'overview_graph_zoom_day': 'day'
    });
  },
  activeKeysZoom: function () {
    return this.chooseSelected({
      'overview_keys_zoom_real_time': 'real_time',
      'overview_keys_zoom_one_hr' : 'one_hr',
      'overview_keys_zoom_day': 'day'
    });
  },
  activeStatsTarget: function () {
    var cell = DAL.cells.currentStatTargetCell;
    if (!cell)
      return null;
    var value = cell.value;
    if (!cell)
      return null;
    return [value.name, value.stats.uri];
  },
  installInterceptor: function (wrapperName, obj, methodName) {
    var self = this;
    var method = obj[methodName];
    var rv = obj[methodName] = function () {
      var args = [method].concat(_.toArray(arguments));
      return self[wrapperName].apply(self, args);
    }
    rv.originalMethod = method;
    return rv;
  },
  interceptAjax: function () {
    this.installInterceptor('interceptedAjax', $, 'ajax');
    this.installInterceptor('interceptedAddBasicAuth', window, 'addBasicAuth');
  },
  interceptedAjax: function (original, options) {
    console.log("intercepted ajax:", options.url, options);
    (new MockedRequest(options)).respond();
  },
  interceptedAddBasicAuth: function (original, xhr, login, password) {
    if (!xhr.fakeAddBasicAuth) {
      throw new Error("incomplete hook.js installation");
    }
    xhr.fakeAddBasicAuth(login, password);
  }
};

var ajaxRespondDelay = 100;

function dateToFakeRFC1123(date) {
  function twoDigits(n) {
    return String(100 + n).slice(1);
  }
  var monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return ['XXX, ', twoDigits(date.getUTCDate()), ' ',
          monthNames[date.getUTCMonth()], ' ', date.getUTCFullYear(), ' ',
          twoDigits(date.getHours()),':',twoDigits(date.getMinutes()),':', twoDigits(date.getSeconds()),
         ' GMT'].join('');
}

var MockedRequest = mkClass({
  initialize: function (options) {
    if (options.type == null) {
      options = _.clone(options);
      options.type = 'GET';
    }
    if (options.type != 'GET' && options.type != 'POST' && options.type != 'DELETE') {
      throw new Error("unknown method: " + options.type);
    }

    this.options = options;

    this.fakeXHR = {
      requestHeaders: [],
      setRequestHeader: function () {
        this.requestHeaders.push(_.toArray(arguments));
      },
      getResponseHeader: function (header) {
        header = header.toLowerCase();
        switch (header) {
        case 'date':
          return dateToFakeRFC1123(new Date())
        }
      },
      fakeAddBasicAuth: function (login, password) {
        this.login = login;
        this.password = password;
      }
    }

    this.backtrace = getBacktrace();

    var url = options.url;
    var hostPrefix = document.location.protocol + ":/" + document.location.host;
    if (url.indexOf(hostPrefix) == 0)
      url = url.substring(hostPrefix);
    if (url.indexOf("/") == 0)
      url = url.substring(1);
    if (url.lastIndexOf("/") == url.length - 1)
      url = url.substring(0, url.length - 1);

    this.url = url;

    var path = url.split('?')[0].split("/")
    this.path = path;
  },
  fakeResponse: function (data) {
    if (data instanceof Function) {
      data.call(null, _.bind(this.fakeResponse, this));
      return;
    }
    console.log("responded with: ", data);
    this.responded = true;
    if (this.options.success)
      this.options.success(data, 'success', this.fakeXHR);
  },
  authError: (function () {
    try {
      throw new Error("autherror")
    } catch (e) {
      return e;
    }
  })(),
  respond: function () {
    if  (this.options.url.indexOf("&etag") > 0) {
      setTimeout($m(this, 'respondForReal'), 5000);
    } else if (this.options.async != false)
      setTimeout($m(this, 'respondForReal'), window.ajaxRespondDelay);
    else
      this.respondForReal();
  },
  findResponseFor: function (method, path, body) {
    var x = this.routes.x;
    var foundResp;
    var routeArgs;
    _.each(this.routes, function (rt) {
      var key = rt[0];
      if (key[0] != method)
        return;
      var pattern = key[1];
      if (pattern.length != path.length)
        return;
      var args = [];
      for (var i = pattern.length-1; i >= 0; i--) {
        var value = pattern[i];
        if (value == x)
          args.push(path[i]);
        else if (value != path[i])
          return;
      }
      foundResp = rt[1];
      if (rt[2])
        foundResp = rt[2].apply(this, [foundResp].concat(args));
      routeArgs = args;
    });
    if (body)
      return body.call(this, foundResp, routeArgs);
    return foundResp;
  },
  executeRouteResponse: function (foundResp, routeArgs) {
    if (_.isFunction(foundResp)) {
      if (functionArgumentNames(foundResp)[0] == "$data")
        routeArgs.unshift(this.deserialize());
      foundResp = foundResp.apply(this, routeArgs);
      if (this.responded)
        return;
      if (foundResp == null)
        foundResp = "";
    }
    if (!_.isFunction(foundResp)) {
      foundResp = _.clone(foundResp);
    }
    return foundResp;
  },
  respondForReal: function () {
    if ($.ajaxSettings.beforeSend)
      $.ajaxSettings.beforeSend(this.fakeXHR, {url: "/"});

    this.findResponseFor(this.options.type, this.path, function (foundResp, routeArgs) {
      if (!foundResp) {
        console.log("Bad request is: ", this);
        throw new Error("Unknown ajax request: Method: " + this.options.type + ", Path: " + this.options.url);
      }

      try {
        this.checkAuth();
        foundResp = this.executeRouteResponse(foundResp, routeArgs);
        if (!this.responded && !this.responseDelayed)
          this.fakeResponse(foundResp);
      } catch (e) {
        if (e !== this.authError) {
          throw e;
        }

        this.fakeXHR.status = 401;
        // auth error
        if (this.options.error) {
          this.options.error(this.fakeXHR, 'error');
        } else
          $.ajaxSettings.error(this.fakeXHR, 'error');
      }
    });
  },
  checkAuth: function () {
  },
  checkAuthReal: function () {
    if (this.fakeXHR.login != 'admin' || this.fakeXHR.password != 'admin')
      throw this.authError;
  },

  deserialize: function (data) {
    data = data || this.options.data;
    return deserializeQueryString(data);
  },

  errorResponse: function (resp) {
    var self = this;
    self.responded = true;
    var fakeXHR = {status: 400,
                   responseText: JSON.stringify(resp)};
    _.defer(function () {
      self.options.error(fakeXHR, 'error');
    });
  },

  doHandlePoolsDefaultPost: function (params) {
    var errors = {};

    if (isBlank(params['memoryQuota'])) {
      errors.memoryQuota = 'must have a memory quota';
    }

    if (_.keys(errors).length) {
      return this.errorResponse(errors);
    }

    this.fakeResponse('');
  },

  handlePoolsDefaultPost: function () {
    var params = this.deserialize()
    console.log("params: ", params);

    return this.doHandlePoolsDefaultPost(params);
  },

  handleBucketsPost: function () {
    var rv = ServerStateMock.handleBucketsPost(this);
    this.fakeResponse(rv);
  },

  handleJoinCluster: function () {
    var params = this.deserialize()
    console.log("params: ", params);
    var ok = true;

    _.each(('clusterMemberHostIp clusterMemberPort user password').split(' '), function (name) {
      if (!params[name] || !params[name].length) {
        ok = false;
      }
    });

    if (ok) {
      this.fakeResponse('');
    } else
      this.errorResponse(['error1', 'error2']);
  },

  handleBucketRemoval: function () {
    ServerStateMock.handleBucketRemoval(self.options);
    return this.fakeResponse('');
  },
  handleStats: function () {
    var params = this.options['data'];
    var zoom = params['zoom'] || 'minute'
    var samplesSelection = [[3,14,23,52,45,25,23,22,50,67,59,55,54,41,36,35,26,61,72,49,60,52,45,25,23,22,50,67,59,55,14,23,52,45,25,23,22,50,67,59,55,54,41,36,35,26,61,72,49,60,52,45,25,23,22,50,67,59,55],
                            [23,14,45,64,41,45,43,25,14,11,18,36,64,76,86,86,79,78,55,59,49,52,45,25,23,22,50,67,59,55,14,45,64,41,45,43,25,14,11,18,36,64,76,86,86,79,78,55,59,49,52,45,25,23,22,50,67,59,55],
                            [42,65,42,63,81,87,74,84,56,44,71,64,49,48,55,46,37,46,64,33,18,52,45,25,23,22,50,67,59,55,65,42,63,81,87,74,84,56,44,71,64,49,48,55,46,37,46,64,33,18,52,45,25,23,22,50,67,59,55],
                            [61,65,64,75,77,57,68,76,64,61,66,63,68,37,32,60,72,54,43,41,55,52,45,25,23,22,50,67,59,55,65,64,75,77,57,68,76,64,61,66,63,68,37,32,60,72,54,43,41,55,52,45,25,23,22,50,67,59,55]];
    var samples = {};
    var recognizedStats = [];
    var statsDirectory = MockedRequest.prototype.findResponseFor("GET", ["pools", "default", "buckets", 4, "statsDirectory"]);
    var allStatsInfos = [].concat.apply([], _.pluck(statsDirectory.blocks, 'stats'));
    _.each(allStatsInfos, function (info) {
      recognizedStats.push(info.name)
    });

    for (var idx in recognizedStats) {
      var data = samplesSelection[(idx + zoom.charCodeAt(0))%4];
      samples[recognizedStats[idx]] = _.map(data, function (i) {return i*10E9});
    }
    var samplesSize = samplesSelection[0].length;

    var samplesInterval = 1000;

    switch (zoom) {
    case 'minute':
      break;
    case 'hour':
      samplesInterval = 60000;
      break;
    default:
      samplesInterval = 1440000;
    }

    var now = (new Date()).valueOf();
    var base = (new Date(2010, 1, 1)).valueOf();
    var lastSampleTstamp = Math.ceil((now - base) / 1000) * 1000;

    if (samplesInterval == 1000) {
      var rotates = ((now / 1000) >> 0) % samplesSize;
      var newSamples = {};
      for (var k in samples) {
        var data = samples[k];
        newSamples[k] = data.concat(data).slice(rotates, rotates + samplesSize);
      }
      samples = newSamples;
    }

    samples.timestamp = _.range(lastSampleTstamp - samplesSelection[0].length * samplesInterval, lastSampleTstamp, samplesInterval);

    var lastSampleT = params['haveTStamp']
    if (lastSampleT) {
      lastSampleT = parseInt(lastSampleT, 10);
      var index = _.lastIndexOf(samples.timestamp, lastSampleT);
      if (index == samples.timestamp.length-1) {
        var self = this;
        _.delay(function () {
          self.fakeResponse(self.handleStats());
        }, 1000);
        this.responseDelayed = true;
        return;
      }
      if (index >= 0) {
        for (var statName in samples) {
          samples[statName] = samples[statName].slice(index+1);
        }
      }
    }

    if (zoom == 'month') {
      for (var key in samples) {
        samples[key] = [];
      }
    }

    return {hot_keys: [{name: "user:image:value",
                        ops: 10000,
                        evictions: 10,
                        ratio: 0.89,
                        bucket: "Excerciser application"},
                       {name: "user:image:value2",
                        ops: 10000,
                        ratio: 0.90,
                        evictions: 11,
                        bucket: "Excerciser application"},
                       {name: "user:image:value3",
                        ops: 10000,
                        ratio: 0.91,
                        evictions: 12,
                        bucket: "Excerciser application"},
                       {name: "user:image:value4",
                        ops: 10000,
                        ratio: 0.92,
                        evictions: 13,
                        bucket: "Excerciser application"}],
            op: {
              isPersistent: true,
              lastTStamp: samples.timestamp.slice(-1)[0],
              tstampParam: lastSampleT,
              interval: samplesInterval,
              samplesCount: 60,
              samples: samples
            }};
  },
  __defineRouting: function () {
    var x = {}
    function mkHTTPMethod(method) {
      return function () {
        return [method, _.toArray(arguments)];
      }
    }

    var get = mkHTTPMethod("GET");
    var post = mkHTTPMethod("POST");
    var del = mkHTTPMethod("DELETE");
    function method(name) {
      return function () {
        return this[name].apply(this, arguments);
      }
    }

    // for optional params
    function opt(name) {
      name = new String(name);
      name.__opt = true;
      return name;
    }

    function expectParams() {
      var expectedParams = _.toArray(arguments);

      var chainedRoute = expectedParams[0];
      if (!_.isString(chainedRoute))
        expectedParams.shift();
      else
        chainedRoute = null;

      var mustParams = [], optionalParams = [];
      _.each(expectedParams, function (p) {
        if (p.__opt)
          optionalParams.push(p.valueOf());
        else
          mustParams.push(p);
      });

      var difference = function (a, b) {
        return _.reject(a, function (e) {
          return _.include(b, e);
        });
      }

      return function () {
        var params = this.deserialize();
        var keys = _.keys(params);

        var missingParams = difference(mustParams, keys);
        if (missingParams.length) {
          var msg = "Missing required parameter(s): " + missingParams.join(', ') + '\nHave: ' + keys.join(',');
          alert("hooks.js: " + msg);
          throw new Error(msg);
        }

        var unexpectedParams = difference(difference(keys, mustParams), optionalParams);
        if (unexpectedParams.length) {
          var msg = "Post has unexpected parameter(s): " + unexpectedParams.join(', ');
          alert("hooks.js: " + msg);
          throw new Error(msg);
        }

        if (chainedRoute)
          return this.executeRouteResponse(chainedRoute, _.toArray(arguments));
      }
    }

    var rv = [
      [get("internalSettings"), {
        "indexAwareRebalanceDisabled": true,
        "rebalanceIndexWaitingDisabled": false,
        "rebalanceIndexPausingDisabled": true
      }],
      [post("internalSettings"), method('doNothingPOST')],
      [post("logClientError"), method('doNothingPOST')],
      [get("logs"), {list: [{type: "info", code: 1, module: "ns_config_log", tstamp: 1265358398000, shortText: "message", text: "config changed"},
                            {type: "info", code: 1, module: "ns_node_disco", tstamp: 1265358398000, shortText: "message", text: "otp cookie generated: bloeahcdnsddpotx"},
                            {type: "info", code: 1, module: "ns_config_log", tstamp: 1265358398000, shortText: "message", text: "config changed"},
                            {type: "info", code: 1, module: "ns_config_log", tstamp: 1265358399000, shortText: "message", text: "config changed"}]}],
      [get("alerts"), {limit: 15,
                       settings: {updateURI: "/alerts/settings"},
                       list: [{number: 3,
                               type: "info",
                               tstamp: 1259836260000,
                               shortText: "Above Average Operations per Second",
                               text: "Licensing, capacity, Couchbase issues, etc."},
                              {number: 2,
                               type: "attention",
                               tstamp: 1259836260000,
                               shortText: "New Node Joined Pool",
                               text: "A new node is now online"},
                              {number: 1,
                               type: "warning",
                               tstamp: 1259836260000,
                               shortText: "Server Node Down",
                               text: "Server node is no longer available"}]}],


      [get("settings", "web"), {port:8091,
                                username:"admin",
                                password:""}],
      [get("settings", "advanced"), {alerts: {email:"alk@tut.by",
                                              sender: "alk@tut.by",
                                              email_server: {user:"",
                                                             pass:"",
                                                             addr:"",
                                                             port:"",
                                                             encrypt:"0"},
                                              sendAlerts:"0",
                                              alerts: {
                                                server_down:"1",
                                                server_unresponsive:"1",
                                                server_up:"1",
                                                server_joined:"1",
                                                server_left:"1",
                                                bucket_created:"0",
                                                bucket_deleted:"1",
                                                bucket_auth_failed:"1"}},
                                     ports:{proxyPort:11213,
                                            directPort:11212}}],
      [get("settings", "stats"), {sendStats:false}],
      [get("settings", "autoFailover"), {"enabled":false,"timeout":30,"count":0}],
      [get("settings", "alerts"), {"recipients":["root@localhost"],"sender":"membase@localhost","enabled":true,"emailServer":{"user":"","pass":"","host":"localhost","port":25,"encrypt":false},"alerts":["auto_failover_node","auto_failover_maximum_reached","auto_failover_other_nodes_down","auto_failover_cluster_too_small"]}],
      [get("pools"), function () {
        return ServerStateMock.pools(this);
      }],
      [get("pools", x), function () {
        var self = this;
        if (self.url.indexOf("etag") >= 0) {
          self.responseDelayed = true;
          _.delay(function () {
            self.fakeResponse(ServerStateMock.poolDetails());
          }, 20000);
          return;
        }
        return ServerStateMock.poolDetails();
      }],
      [get("nodeStatuses"), function () {
        return ServerStateMock.nodeStatuses();
      }],
      [get("pools", "default", "buckets"), function () {
        return ServerStateMock.bucketsList();
      }],
      [get("pools", "default", "buckets", x), function (x) {
        return ServerStateMock.bucketDetails(this.path);
      }],
      [get("pools", "default", "buckets", x, "statsDirectory"), {
        "blocks": [
          {"blockName":"Server Resources","serverResources":true,"extraCSSClasses":"server_resources",
            "stats":[
              {"specificStatsURL":"/pools/default/buckets/default/stats/swap_used","name":"swap_used","title":"swap usage","desc":"Amount of swap space in use on this server (B=bytes, M=megabytes, G=gigabytes)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/mem_actual_free","name":"mem_actual_free","title":"free RAM","desc":"Amount of RAM available on this server (B=bytes, M=megabytes, G=gigabytes)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/cpu_utilization_rate","name":"cpu_utilization_rate","title":"CPU utilization %","desc":"Percentage of CPU in use across all available cores on this server","maxY":100},
              {"specificStatsURL":"/pools/default/buckets/default/stats/curr_connections","name":"curr_connections","title":"connections","desc":"Number of connections to this server including connections from external drivers, proxies, TAP requests and internal statistic gathering (measured from curr_connections)"}
            ]},
          {"blockName":"Summary",
            "stats":[
              {"specificStatsURL":"/pools/default/buckets/default/stats/ops","title":"ops per second","name":"ops","desc":"Total amount of operations per second to this bucket (measured from cmd_get + cmd_set + incr_misses + incr_hits + decr_misses + decr_hits + delete_misses + delete_hits)","default":true},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_cache_miss_rate","title":"cache miss ratio","name":"ep_cache_miss_rate","desc":"Percentage of reads per second to this bucket from disk as opposed to RAM (measured from 100 - (gets - ep_bg_fetches) * 100 / gets)","maxY":100},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_ops_create","title":"creates per sec.","name":"ep_ops_create","desc":"Number of new items created per second in this bucket (measured from vb_active_ops_create + vb_replica_ops_create + vb_pending_ops_create)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_ops_update","title":"updates per sec.","name":"ep_ops_update","desc":"Number of existing items mutated per second in this bucket (measured from vb_active_ops_update + vb_replica_ops_update + vb_pending_ops_update)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_bg_fetched","title":"disk reads per sec.","name":"ep_bg_fetched","desc":"Number of reads per second from disk for this bucket (measured from ep_bg_fetched)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tmp_oom_errors","title":"temp OOM per sec.","name":"ep_tmp_oom_errors","desc":"Number of back-offs sent per second to drivers due to \"out of memory\" situations from this bucket (measured from ep_tmp_oom_errors)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/cmd_get","title":"gets per sec.","name":"cmd_get","desc":"Number of reads (get operations) per second from this bucket (measured from cmd_get)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/cmd_set","title":"sets per sec.","name":"cmd_set","desc":"Number of writes (set operations) per second to this bucket (measured from cmd_set)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/cas_hits","title":"CAS ops per sec.","name":"cas_hits","desc":"Number of operations with a CAS id per second for this bucket (measured from cas_hits)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/delete_hits","title":"deletes per sec.","name":"delete_hits","desc":"Number of delete operations per second for this bucket (measured from delete_hits)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/curr_items","title":"items","name":"curr_items","desc":"Number of unique items in this bucket - only active items, not replica (measured from curr_items)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/disk_write_queue","title":"disk write queue","name":"disk_write_queue","desc":"Number of items waiting to be written to disk in this bucket (measured from ep_queue_size+ep_flusher_todo)"}
            ]},
          {"blockName":"vBucket Resources","extraCSSClasses":"withtotal closed",
            "columns":["Active","Replica","Pending","Total"],
            "stats":[
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_active_num","title":"vBuckets","name":"vb_active_num","desc":"Number of vBuckets in the \"active\" state for this bucket (measured from vb_active_num)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_replica_num","title":"vBuckets","name":"vb_replica_num","desc":"Number of vBuckets in the \"replica\" state for this bucket (measured from vb_replica_num)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_pending_num","title":"vBuckets","name":"vb_pending_num","desc":"Number of vBuckets in the \"pending\" state for this bucket and should be transient during rebalancing (measured from vb_pending_num)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_vb_total","title":"vBuckets","name":"ep_vb_total","desc":"Total number of vBuckets for this bucket (measured from ep_vb_total)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/curr_items","title":"items","name":"curr_items","desc":"Number of items in \"active\" vBuckets in this bucket (measured from curr_items)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_replica_curr_items","title":"items","name":"vb_replica_curr_items","desc":"Number of items in \"replica\" vBuckets in this bucket (measured from vb_replica_curr_items)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_pending_curr_items","title":"items","name":"vb_pending_curr_items","desc":"Number of items in \"pending\" vBuckets in this bucket and should be transient during rebalancing (measured from vb_pending_curr_items)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/curr_items_tot","title":"items","name":"curr_items_tot","desc":"Total number of items in this bucket (measured from curr_items_tot)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_active_resident_items_ratio","title":"resident %","name":"vb_active_resident_items_ratio","desc":"Percentage of active items cached in RAM in this bucket (measured from vb_active_resident_items_ratio)","maxY":100},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_replica_resident_items_ratio","title":"resident %","name":"vb_replica_resident_items_ratio","desc":"Percentage of replica items cached in RAM in this bucket (measured from vb_replica_resident_items_ratio)","maxY":100},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_pending_resident_items_ratio","title":"resident %","name":"vb_pending_resident_items_ratio","desc":"Percentage of replica items cached in RAM in this bucket (measured from vb_replica_resident_items_ratio)","maxY":100},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_resident_items_rate","title":"resident %","name":"ep_resident_items_rate","desc":"Percentage of all items cached in RAM in this bucket (measured from ep_resident_items_rate)","maxY":100},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_active_ops_create","title":"new items per sec.","name":"vb_active_ops_create","desc":"New items per second being inserted into \"active\" vBuckets in this bucket (measured from vb_active_ops_create)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_replica_ops_create","title":"new items per sec.","name":"vb_replica_ops_create","desc":"New items per second being inserted into \"replica\" vBuckets in this bucket (measured from vb_replica_ops_create"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_pending_ops_create","title":"new items per sec.","name":"vb_pending_ops_create","desc":"New items per second being instead into \"pending\" vBuckets in this bucket and should be transient during rebalancing (measured from vb_pending_ops_create)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_ops_create","title":"new items per sec.","name":"ep_ops_create","desc":"Total number of new items being inserted into this bucket (measured from ep_ops_create)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_active_eject","title":"ejections per sec.","name":"vb_active_eject","desc":"Number of items per second being ejected to disk from \"active\" vBuckets in this bucket (measured from vb_active_eject)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_replica_eject","title":"ejections per sec.","name":"vb_replica_eject","desc":"Number of items per second being ejected to disk from \"replica\" vBuckets in this bucket (measured from vb_replica_eject)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_pending_eject","title":"ejections per sec.","name":"vb_pending_eject","desc":"Number of items per second being ejected to disk from \"pending\" vBuckets in this bucket and should be transient during rebalancing (measured from vb_pending_eject)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_num_value_ejects","title":"ejections per sec.","name":"ep_num_value_ejects","desc":"Total number of items per second being ejected to disk in this bucket (measured from ep_num_value_ejects)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_active_itm_memory","title":"user data in RAM","name":"vb_active_itm_memory","desc":"Amount of active user data cached in RAM in this bucket (measured from vb_active_itm_memory)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_replica_itm_memory","title":"user data in RAM","name":"vb_replica_itm_memory","desc":"Amount of replica user data cached in RAM in this bucket (measured from vb_replica_itm_memory)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_pending_itm_memory","title":"user data in RAM","name":"vb_pending_itm_memory","desc":"Amount of pending user data cached in RAM in this bucket and should be transient during rebalancing (measured from vb_pending_itm_memory)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_kv_size","title":"user data in RAM","name":"ep_kv_size","desc":"Total amount of user data cached in RAM in this bucket (measured from ep_kv_size)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_active_meta_data_memory","title":"metadata in RAM","name":"vb_active_meta_data_memory","desc":"Amount of active item metadata consuming RAM in this bucket (measured from vb_active_meta_data_memory)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_replica_meta_data_memory","title":"metadata in RAM","name":"vb_replica_meta_data_memory","desc":"Amount of replica item metadata consuming in RAM in this bucket (measured from vb_replica_meta_memory)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_pending_meta_data_memory","title":"metadata in RAM","name":"vb_pending_meta_data_memory","desc":"Amount of pending item metadata consuming RAM in this bucket and should be transient during rebalancing (measured from vb_pending_meta_memory)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_meta_data_memory","title":"metadata in RAM","name":"ep_meta_data_memory","desc":"Total amount of item  metadata consuming RAM in this bucket (measured from ep_meta_data_memory)"}
            ]},
          {"blockName":"Disk Queues","extraCSSClasses":"withtotal closed",
            "columns":["Active","Replica","Pending","Total"],
            "stats":[
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_active_queue_size","title":"items","name":"vb_active_queue_size","desc":"Number of active items waiting to be written to disk in this bucket (measured from vb_active_queue_size)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_replica_queue_size","title":"items","name":"vb_replica_queue_size","desc":"Number of replica items waiting to be written to disk in this bucket (measured from vb_replica_queue_size)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_pending_queue_size","title":"items","name":"vb_pending_queue_size","desc":"Number of pending items waiting to be written to disk in this bucket and should be transient during rebalancing  (measured from vb_pending_queue_size)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_diskqueue_items","title":"items","name":"ep_diskqueue_items","desc":"Total number of items waiting to be written to disk in this bucket (measured from ep_diskqueue_items)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_active_queue_fill","title":"fill rate","name":"vb_active_queue_fill","desc":"Number of active items per second being put on the active item disk queue in this bucket (measured from vb_active_queue_fill)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_replica_queue_fill","title":"fill rate","name":"vb_replica_queue_fill","desc":"Number of replica items per second being put on the replica item disk queue in this bucket (measured from vb_replica_queue_fill)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_pending_queue_fill","title":"fill rate","name":"vb_pending_queue_fill","desc":"Number of pending items per second being put on the pending item disk queue in this bucket and should be transient during rebalancing (measured from vb_pending_queue_fill)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_diskqueue_fill","title":"fill rate","name":"ep_diskqueue_fill","desc":"Total number of items per second being put on the disk queue in this bucket (measured from ep_diskqueue_fill)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_active_queue_drain","title":"drain rate","name":"vb_active_queue_drain","desc":"Number of active items per second being written to disk in this bucket (measured from vb_pending_queue_drain)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_replica_queue_drain","title":"drain rate","name":"vb_replica_queue_drain","desc":"Number of replica items per second being written to disk in this bucket (measured from vb_replica_queue_drain)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_pending_queue_drain","title":"drain rate","name":"vb_pending_queue_drain","desc":"Number of pending items per second being written to disk in this bucket and should be transient during rebalancing (measured from vb_pending_queue_drain)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_diskqueue_drain","title":"drain rate","name":"ep_diskqueue_drain","desc":"Total number of items per second being written to disk in this bucket (measured from ep_diskqueue_drain)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_avg_active_queue_age","title":"average age","name":"vb_avg_active_queue_age","desc":"Average age in seconds of active items in the active item queue for this bucket (measured from vb_avg_active_queue_age)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_avg_replica_queue_age","title":"average age","name":"vb_avg_replica_queue_age","desc":"Average age in seconds of replica items in the replica item queue for this bucket (measured from vb_avg_replica_queue_age)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_avg_pending_queue_age","title":"average age","name":"vb_avg_pending_queue_age","desc":"Average age in seconds of pending items in the pending item queue for this bucket and should be transient during rebalancing (measured from vb_avg_pending_queue_age)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/vb_avg_total_queue_age","title":"average age","name":"vb_avg_total_queue_age","desc":"Average age in seconds of all items in the disk write queue for this bucket (measured from vb_avg_total_queue_age)"}
            ]},
          {"blockName":"Tap Queues","extraCSSClasses":"withtotal closed",
            "columns":["Replication","Rebalance","Clients","Total"],
            "stats":[
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_replica_count","title":"TAP senders","name":"ep_tap_replica_count","desc":"Number of internal replication TAP queues in this bucket (measured from ep_tap_replica_count)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_rebalance_count","title":"TAP senders","name":"ep_tap_rebalance_count","desc":"Number of internal rebalancing TAP queues in this bucket (measured from ep_tap_rebalance_count)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_user_count","title":"TAP senders","name":"ep_tap_user_count","desc":"Number of internal \"user\" TAP queues in this bucket (measured from ep_tap_user_count)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_total_count","title":"TAP senders","name":"ep_tap_total_count","desc":"Total number of internal TAP queues in this bucket (measured from ep_tap_total_count)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_replica_qlen","title":"items","name":"ep_tap_replica_qlen","desc":"Number of items in the replication TAP queues in this bucket (measured from ep_tap_replica_qlen)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_rebalance_qlen","title":"items","name":"ep_tap_rebalance_qlen","desc":"Number of items in the rebalance TAP queues in this bucket (measured from ep_tap_rebalance_qlen)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_user_qlen","title":"items","name":"ep_tap_user_qlen","desc":"Number of items in \"user\" TAP queues in this bucket (measured from ep_tap_user_qlen)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_total_qlen","title":"items","name":"ep_tap_total_qlen","desc":"Total number of items in TAP queues in this bucket (measured from ep_tap_total_qlen)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_replica_queue_drain","title":"drain rate","name":"ep_tap_replica_queue_drain","desc":"Number of items per second being sent over replication TAP connections to this bucket, i.e. removed from queue (measured from ep_tap_replica_queue_drain)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_rebalance_queue_drain","title":"drain rate","name":"ep_tap_rebalance_queue_drain","desc":"Number of items per second being sent over rebalancing TAP connections to this bucket, i.e. removed from queue (measured from ep_tap_rebalance_queue_drain)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_user_queue_drain","title":"drain rate","name":"ep_tap_user_queue_drain","desc":"Number of items per second being sent over \"user\" TAP connections to this bucket, i.e. removed from queue (measured from ep_tap_user_queue_drain)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_total_queue_drain","title":"drain rate","name":"ep_tap_total_queue_drain","desc":"Total number of items per second being sent over TAP connections to this bucket (measured from ep_tap_total_queue_drain)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_replica_queue_backoff","title":"back-off rate","name":"ep_tap_replica_queue_backoff","desc":"Number of back-offs received per second while sending data over replication TAP connections to this bucket (measured from ep_tap_replica_queue_backoff)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_rebalance_queue_backoff","title":"back-off rate","name":"ep_tap_rebalance_queue_backoff","desc":"Number of back-offs received per second while sending data over rebalancing TAP connections to this bucket (measured from ep_tap_rebalance_queue_backoff)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_user_queue_backoff","title":"back-off rate","name":"ep_tap_user_queue_backoff","desc":"Number of back-offs received per second while sending data over \"user\" TAP connections to this bucket (measured from ep_tap_user_queue_backoff)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_total_queue_backoff","title":"back-off rate","name":"ep_tap_total_queue_backoff","desc":"Total number of back-offs received per second while sending data over TAP connections to this bucket (measured from ep_tap_total_queue_backoff)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_replica_queue_backfillremaining","title":"backfill remaining","name":"ep_tap_replica_queue_backfillremaining","desc":"Number of items in the backfill queues of replication TAP connections for this bucket (measured from ep_tap_replica_queue_backfillremaining)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_rebalance_queue_backfillremaining","title":"backfill remaining","name":"ep_tap_rebalance_queue_backfillremaining","desc":"Number of items in the backfill queues of rebalancing TAP connections to this bucket (measured from ep_tap_rebalance_queue_backfillreamining)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_user_queue_backfillremaining","title":"backfill remaining","name":"ep_tap_user_queue_backfillremaining","desc":"Number of items in the backfill queues of \"user\" TAP connections to this bucket (measured from ep_tap_user_queue_backfillremaining)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_total_queue_backfillremaining","title":"backfill remaining","name":"ep_tap_total_queue_backfillremaining","desc":"Total number of items in the backfill queues of TAP connections to this bucket (measured from ep_tap_total_queue_backfillremaining)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_replica_queue_itemondisk","title":"remaining on disk","name":"ep_tap_replica_queue_itemondisk","desc":"Number of items still on disk to be loaded for replication TAP connections to this bucket (measured from ep_tap_replica_queue_itemondisk)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_rebalance_queue_itemondisk","title":"remaining on disk","name":"ep_tap_rebalance_queue_itemondisk","desc":"Number of items still on disk to be loaded for rebalancing TAP connections to this bucket (measured from ep_tap_rebalance_queue_itemondisk)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_user_queue_itemondisk","title":"remaining on disk","name":"ep_tap_user_queue_itemondisk","desc":"Number of items still on disk to be loaded for \"client\" TAP connections to this bucket (measured from ep_tap_user_queue_itemondisk)"},
              {"specificStatsURL":"/pools/default/buckets/default/stats/ep_tap_total_queue_itemondisk","title":"remaining on disk","name":"ep_tap_total_queue_itemondisk","desc":"Total number of items still on disk to be loaded for TAP connections to this bucket (measured from ep_tao_total_queue_itemonsidk)"}
            ]}
          ]}
      ],
      [get("pools", "default", "buckets", x, "stats"), method('handleStats')],
      [get("pools", "default", "buckets", x, "nodes"), {
        servers: [
                    {
                        "hostname": "ns_1@127.0.0.1",
                        "uri": "/pools/default/buckets/default/nodes/ns_1@127.0.0.1",
                        "stats": {
                            "uri": "/pools/default/buckets/default/nodes/ns_1%40127.0.0.1/stats"
                        }
                    }
                 ]
        }],
      [get("pools", "default", "buckets", x, "nodes", x), {
        hostname:"ns_1@127.0.0.1",
        stats: {uri: '/pools/default/buckets/4/nodes/ns_1@127.0.0.1/stats'}
      }],
      [get("pools", "default", "buckets", x, "nodes", x, "stats"), {
          server: "ns_1@127.0.0.1",
          op: {
            samples: {
              timestamp: [1281667776000.0,1281667780000.0,1281667784000.0,1281667788000.0,
                           1281667796000.0,1281667800000.0,1281667804000.0,1281667809100.0],
              "hit_ratio":[0,0,0,0,0,0,100.0,100.0],
              "ep_cache_hit_rate":[0,0,0,0,0,0,100.0,100.0],
              "ep_resident_items_rate":[12.283674058456635,12.283674058456635,12.283674058456635,12.283674058456635,12.283674058456635,12.283674058456635,12.283674058456635]
            },
            samplesCount: 60,
            isPersistent: true,
            lastTStamp: 0,
            interval: 1000
          }
        }
      ],
      [get("pools", "default", "remoteClusters"), [
        {name: "kyiv",
         uri: "/pools/default/remoteClusters/kyiv",
         validateURI: "/pools/default/remoteClusters/kyiv?just_validate=1",
         hostname: "kyiv-mb01.yyy.com",
         username: "yaraslau-the-wise"},
        {name: "london",
         uri: "/pools/default/remoteClusters/london",
         validateURI: "/pools/default/remoteClusters/london?just_validate=1",
         hostname: "london-mb04.yyy.com:80",
         username: "victoria"}
      ]],
      [del("pools", "default", "remoteClusters", x), method('doNothingPOST')],
      [post("pools", "default", "remoteClusters", x), method('doNothingPOST')],
      [post("pools", "default", "remoteClusters"), method('doNothingPOST')],
      [get("pools", "default", "overviewStats"), {
        "timestamp":[1281667776000.0,1281667780000.0,1281667784000.0,1281667788000.0,1281667792000.0,
                     1281667796000.0,1281667800000.0,1281667804000.0,1281667809100.0,1281667812000.0,
                     1281667816000.0,1281667820000.0,1281667824000.0,1281667828000.0,1281667832000.0,
                     1281667836000.0,1281667840000.0,1281667844000.0,1281667848000.0,1281667852000.0,
                     1281667856000.0,1281667860000.0,1281667864000.0,1281667868000.0,1281667872000.0,
                     1281667876000.0,1281667880000.0,1281667884000.0,1281667888000.0,1281667892000.0,
                     1281667896000.0,1281667900000.0,1281667904000.0,1281667908000.0,1281667912000.0,
                     1281667916000.0,1281667920000.0,1281667924000.0,1281667928000.0,1281667932000.0,
                     1281667936000.0,1281667940000.0,1281667944000.0,1281667948000.0,1281667952000.0,
                     1281667956000.0,1281667960000.0,1281667964000.0,1281667968000.0,1281667972000.0,
                     1281667976000.0,1281667980000.0,1281667984000.0,1281667988000.0,1281667992000.0,
                     1281667996000.0,1.281668e+12,1281668004000.0,1281668008000.0,1281668012000.0,
                     1281668016000.0,1281668020000.0,1281668024000.0,1281668028000.0,1281668032000.0,
                     1281668036000.0,1281668040000.0,1281668044000.0,1281668048000.0,1281668052000.0,
                     1281668056000.0,1281668060000.0,1281668064000.0,1281668068000.0,1281668072000.0,
                     1281668076000.0,1281668091000.0,1281668084000.0,1281668088000.0,1281668092000.0,
                     1281668096000.0,1281668100000.0,1281668104000.0,1281668108000.0,1281668112000.0,
                     1281668116000.0,1281668120000.0,1281668124000.0,1281668128000.0,1281668132000.0,
                     1281668136000.0,1281668140000.0,1281668144000.0,1281668148000.0,1281668152000.0,
                     1281668156000.0,1281668160000.0,1281668164000.0,1281668168000.0,1281668172000.0,
                     1281668176000.0,1281668180000.0,1281668184000.0,1281668188000.0,1281668192000.0,
                     1281668196000.0,1281668200000.0,1281668204000.0,1281668208000.0,1281668212000.0,
                     1281668216000.0,1281668220000.0,1281668224000.0,1281668228000.0,1281668232000.0,
                     1281668236000.0,1281668240000.0,1281668244000.0,1281668248000.0,1281668252000.0,
                     1281668256000.0,1281668260000.0,1281668264000.0,1281668268000.0,1281668272000.0,
                     1281668276000.0,1281668280000.0,1281668284000.0,1281668288000.0,1281668292000.0,
                     1281668296000.0,1281668300000.0,1281668304000.0,1281668308000.0,1281668312000.0,
                     1281668316000.0,1281668320000.0,1281668324000.0,1281668328000.0,1281668332000.0,
                     1281668336000.0,1281668340000.0,1281668344000.0,1281668348000.0,1281668352000.0,
                     1281668356000.0,1281668360000.0,1281668364000.0,1281668368000.0,1281668372000.0,
                     1281668376000.0,1281668380000.0,1281668384000.0,1281668388000.0,1281668392000.0,
                     1281668396000.0,1281668400000.0,1281668404000.0,1281668408000.0,1281668412000.0,
                     1281668416000.0,1281668420000.0,1281668424000.0,1281668428000.0,1281668432000.0,
                     1281668436000.0,1281668440000.0,1281668444000.0,1281668448000.0,1281668452000.0,
                     1281668456000.0,1281668460000.0,1281668464000.0,1281668468000.0,1281668472000.0,
                     1281668476000.0,1281668480000.0,1281668484000.0,1281668488000.0,1281668492000.0,
                     1281668496000.0,1281668500000.0,1281668504000.0,1281668508000.0,1281668512000.0,
                     1281668516000.0,1281668520000.0,1281668524000.0,1281668528000.0,1281668532000.0,
                     1281668536000.0,1281668540000.0,1281668544000.0,1281668548000.0,1281668552000.0,
                     1281668556000.0,1281668560000.0,1281668564000.0,1281668568000.0,1281668572000.0,
                     1281668576000.0,1281668580000.0,1281668584000.0,1281668588000.0,1281668592000.0,
                     1281668596000.0,1281668600000.0,1281668604000.0,1281668608000.0,1281668612000.0,
                     1281668616000.0,1281668620000.0,1281668624000.0,1281668628000.0,1281668632000.0,
                     1281668636000.0,1281668640000.0,1281668644000.0,1281668648000.0,1281668652000.0,
                     1281668656000.0,1281668660000.0,1281668664000.0,1281668668000.0,1281668672000.0,
                     1281668676000.0,1281668680000.0,1281668684000.0,1281668688000.0,1281668692000.0,
                     1281668696000.0,1281668700000.0,1281668704000.0,1281668708000.0,1281668712000.0,
                     1281668716000.0,1281668720000.0,1281668724000.0,1281668728000.0,1281668732000.0,
                     1281668736000.0,1281668740000.0,1281668744000.0,1281668748000.0,1281668752000.0,
                     1281668756000.0,1281668760000.0,1281668764000.0,1281668768000.0,1281668772000.0,
                     1281668776000.0,1281668780000.0,1281668784000.0,1281668788000.0,1281668792000.0,
                     1281668796000.0,1281668800000.0,1281668804000.0,1281668809100.0,1281668812000.0,
                     1281668816000.0,1281668820000.0,1281668824000.0,1281668828000.0,1281668832000.0,
                     1281668836000.0,1281668840000.0,1281668844000.0,1281668848000.0,1281668852000.0,
                     1281668856000.0,1281668860000.0,1281668864000.0,1281668868000.0,1281668872000.0,
                     1281668876000.0,1281668880000.0,1281668884000.0,1281668888000.0,1281668892000.0,
                     1281668896000.0,1281668900000.0,1281668904000.0,1281668908000.0,1281668912000.0,
                     1281668916000.0,1281668920000.0,1281668924000.0,1281668928000.0,1281668932000.0,
                     1281668936000.0,1281668940000.0,1281668944000.0,1281668948000.0,1281668952000.0,
                     1281668956000.0,1281668960000.0,1281668964000.0,1281668968000.0,1281668972000.0,
                     1281668976000.0,1281668980000.0,1281668984000.0,1281668988000.0,1281668992000.0]
        ,"ops":[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,2607.5,9020.0,
                9854.25,9710.5,8918.75,9594.75,8892.25,9434.75,6967.25,3639.0,9177.5,9377.75,9011.25,
                9519.0,9223.5,1474.5,7498.5,8855.0,9326.0,9154.5,8642.0,5101.5,8223.5,9365.0,9382.0,
                8912.5,8975.75,5141.25,5978.5,9366.0,8729.25,9159.0,8897.0,7870.25,3584.75,8533.5,
                8677.75,8836.0,8885.75,9119.5,3759.0,8833.25,9235.25,8318.75,8637.0,8976.25,4603.25,
                8219.25,8751.5,9161.25,8839.25,8876.25,6152.5,5223.75,9226.5,9223.75,8431.5,9095.75,
                8554.25,3864.5,9203.25,8962.25,8850.25,8731.5,9253.5,2675.0,9208.75,8651.75,8958.75,
                8933.25,8400.75,4551.0,6564.5,8662.75,8657.5,8600.5,9229.75,8285.0,5497.5,8542.0,9196.75,
                8838.0,8805.25,8741.0,3923.75,9271.25,8916.25,9351.25,9078.75,8897.25,2241.5,8890.5,8607.0,
                8596.25,8435.75,8671.75,5498.25,7683.5,8346.25,9087.25,9102.5,7829.25,7951.25,5585.75,
                8435.5,9001.25,8609.5,8536.25,8901.25,4348.25,8974.25,9055.0,9155.25,9091.75,8643.0,
                2927.5,8781.0,9307.75,9121.25,8985.75,9093.25,3446.5,8158.25,8935.75,8025.5,8921.0,
                9183.25,6776.25,5491.25,8852.25,8514.75,8944.25,8591.0,8656.25,4389.75,8868.5,8933.5,
                8726.25,8529.0,8509.75,4243.25,8847.5,8535.5,8988.0,8977.5,8698.5,4703.25,7823.75,
                8614.0,9149.25,8647.0,8827.75,6938.25,5126.75,8301.0,8555.25,8338.5,8132.5,7734.25,7414.75,
                8530.75,8274.25,7758.25,7860.0,8174.25,6468.0,7481.0,7969.25,7764.75,7741.0,7914.0,7798.25,
                2663.25,5062.75,8624.0,8028.25,7736.25,7854.5,7438.5,5255.75,48.0,6522.0,7001.0,7395.75,
                7438.0,6927.75,7679.0,6988.5,3196.25,8477.5,8109.5,8637.0,8067.75,7672.75,5839.25,239.25,
                7365.5,6984.75,7577.0,6840.5,7509.25,6461.75,7022.0,801.5,7687.5,8098.5,7434.25,7997.75,7649.5,
                8449.5,3099.5,8252.75,8485.75,8341.75,8545.5,8138.5,7017.75,4279.75,8176.75,7353.75,8477.0,
                7935.75,8380.75,5396.25,2635.0,7837.75,8505.0,8109.5,8591.0,8218.75,7315.25,8358.25,8457.25,
                8379.25,8091.75,8337.75,7163.5,6448.5,7495.0,7386.75,7522.75,8416.75,8004.25,4726.0,464.5,
                6259.25,6514.0,6658.25,5956.5,6643.25,7106.25,6884.25,3513.75,4060.25,7883.75,7754.0,7629.75,
                8199.25,8085.75,6387.25,947.0,7891.25,8236.25,8317.25,8401.25,8291.5,7915.5,7297.0,8308.75,
                8717.0,8071.0,7919.0,8393.25,6234.75,8740.5,8073.75,8237.75,8824.5,8586.25,5796.25,9188.5,
                8442.75,8501.25,8275.75,8754.25,4835.5,8464.5,9132.25,7576.25,8036.25,7586.5],
        "ep_io_num_read":[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
                          0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
                          0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
                          0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
                          0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
                          0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
                          0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
                          0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,29.25,115.25,141.25,
                          173.0,142.0,112.0,115.0,83.5,105.0,84.25,67.0,84.25,59.25,71.0,74.75,68.5,94.75,72.25,
                          84.0,83.75,21.0,87.75,130.25,123.25,147.25,108.25,126.5,69.5,0.0,196.25,157.0,194.0,
                          157.25,135.25,177.25,113.5,96.75,206.25,142.5,183.5,148.0,102.75,98.75,3.5,239.75,206.0,
                          185.75,187.75,149.25,162.5,157.0,35.0,277.75,197.25,196.75,176.75,139.5,181.75,55.75,
                          144.25,136.25,89.75,138.25,95.75,74.0,61.75,82.25,180.75,149.75,143.75,149.0,70.5,65.25,
                          164.25,113.75,149.0,119.25,105.25,102.5,79.75,105.0,81.75,89.25,103.75,71.25,129.5,111.25,
                          121.0,149.0,120.5,148.5,71.5,60.75,1082.0,880.25,911.75,772.0,681.75,855.0,622.75,257.5,
                          449.75,694.5,511.0,608.25,487.25,466.5,331.5,43.0,437.25,334.0,292.0,289.75,201.25,254.25,
                          160.0,178.75,175.25,113.5,134.5,117.5,79.25,111.0,75.75,84.5,72.75,57.0,45.5,51.5,48.75,
                          41.5,41.25,57.0,30.0,53.0,43.0,29.25,32.5,25.0]}],
      [post("pools", "default"), method('handlePoolsDefaultPost')],
      [post("pools", "default", "buckets"), method('handleBucketsPost')],
      [post("pools", "default", "buckets", x), method('handleBucketsPost')],
      [post("pools", "default", "buckets", x, "controller", "doFlush"), method('doNothingPOST')],
      [del("pools", "default", "buckets", x), method('handleBucketRemoval')],
      [get("pools", "default", "buckets", x, "localRandomKey"), {ok: true, key: "asd"}],


      [get("nodes", "self"), {
        "memoryQuota":"",
        "storage":{"ssd":[],
                   "hdd":[{"path":"/opt/couchbase/var/lib/couchbase/data",
                           "quotaMb":"none",
                           "state":"ok"}]},
        "storageTotals": {
          "ram": {
            "usedByData": 259350,
            "quotaTotal": 1832558091,
            "total": 2032558091,
            "used": 1689321472
          },
          "hdd": {
            "usedByData": 25935000,
            "total": 239315349504.0,
            "used": 229742735523.0
          }
        },
        availableStorage: {
          hdd: [{
            path: "/",
            sizeKBytes: 20000,
            usagePercent: 80
          }, {
            path: "/srv",
            sizeKBytes: 20000000,
            usagePercent: 10
          }, {
            path: "/usr",
            sizeKBytes: 2000000,
            usagePercent: 60
          }, {
            path: "/usr/local",
            sizeKBytes: 30000000,
            usagePercent: 0
          }, {
            path: "/home",
            sizeKBytes: 40000000,
            usagePercent: 90
          }
        ]},
        "hostname":"127.0.0.1:8091",
        "version":"1.0.3_98_g5d1f7a2",
        "os":"i386-apple-darwin10.3.0",
        uptime: 86400,
        memoryTotal: 2032574464,
        memoryFree: 1589864960,
        mcdMemoryReserved: 2032574464,
        mcdMemoryAllocated: 89864960,
        "ports":{"proxy":11211,"direct":11210}}],
      [post("nodes", x, "controller", "settings"), expectParams(function ($data) {
        if ($data.memoryQuota && $data.memoryQuota != 'unlimited' && !(/^[0-9]+$/.exec($data.memoryQuota))) {
          this.errorResponse(["invalid memory quota", "second message"]);
        }
      }, opt("memoryQuota"), opt('db_path'), opt('index_path'))], //missing

      [post("node", "controller", "doJoinCluster"), expectParams(method('handleJoinCluster'),
                                                                 "clusterMemberHostIp", "clusterMemberPort",
                                                                 "user", "password")],
      [post("controller", "setupDefaultBucket"),  expectParams(method('handleBucketsPost'),
                                                               "ramQuotaMB", "replicaNumber", "bucketType",
                                                               opt("saslPassword"), opt("authType"))],
      [post("controller", "createReplication"), function () {return ServerStateMock.handleCreateReplication(this)}],
      [post("controller", "ejectNode"), expectParams(method('doNothingPOST'),
                                                     "otpNode")],

      // params are otpNodes of nodes to be kept/ejected
      [post("controller", "rebalance"), expectParams(function () {
        if (__hookParams['rebalanceMismatch']) {
          return this.errorResponse({mismatch: 1});
        }

        var percent = 0;

        MockedRequest.globalData.rebalanceProgress = function () {
          return percent;
        }

        var intervalID = setInterval(function () {
          percent += 0.001;
        }, 50);

        MockedRequest.globalData.setRebalanceStatus('running');
        _.delay(function () {
          console.log("rebalance delay hit!");

          MockedRequest.globalData.rebalanceProgress = null;
          clearInterval(intervalID);

          MockedRequest.globalData.setRebalanceStatus('none');
        }, 80000);
      }, "knownNodes", "ejectedNodes")],
      [get("pools", "default", "rebalanceProgress"), function () {
        var pools = this.findResponseFor("GET", ["pools", "default"]);
        if (pools.rebalanceStatus == 'none') {
          return {status: 'none'};
        }
        var nodes = _.pluck(pools.nodes, 'otpNode');
        var rv = {
          status: pools.rebalanceStatus
        };
        var percent = 0.5;
        if (MockedRequest.globalData.rebalanceProgress) {
          percent = MockedRequest.globalData.rebalanceProgress();
        }
        _.each(nodes, function (name) {
          rv[name] = {progress: percent};
        });
        return rv;
      }],
      [post("controller", "stopRebalance"), method("doNothingPOST")],

      [post("controller", "addNode"), expectParams(method("doNothingPOST"),
                                                   "hostname",
                                                   "user", "password")],
      [post("controller", "failOver"), expectParams(method("doNothingPOST"),
                                                    "otpNode")],
      [post("controller", "reAddNode"), expectParams(method("doNothingPOST"),
                                                     "otpNode")],

      [post("settings", "web"), expectParams(method("doNothingPOST"),
                                             "port", "username", "password")],
      [post("settings", "stats"), method("doNothingPOST")],
      [get("couchBase", "my_replicator", "_design", "_replicator_info", "_view", x),
       function () {return ServerStateMock.handleReplicatorInfos(this);}],
      [get("couchBase", x, "_all_docs"), function () {return ServerStateMock.handleAllDocs(this);}],
      // [get("couchBase", x, "_design", x, "_view", x), function () {return ServerStateMock.handleAllDocs(this);}],
      [get("couchBase", x, "asd"), function () {return ServerStateMock.handleAnyDoc(this);}],
    ];

    rv.x = x;
    return rv;
  },
  doNothingPOST: function () {
  }
});

MockedRequest.prototype.globalData = MockedRequest.globalData = {
  findResponseFor: function (method, path) {
    return MockedRequest.prototype.findResponseFor(method, path);
  },
  setRebalanceStatus: function (status) {
    var pools = this.findResponseFor("GET", ["pools", "default"]);
    pools.rebalanceStatus = status;
  }
};


;(function () {
  MockedRequest.prototype.routes = MockedRequest.prototype.__defineRouting();
})();

var ServerStateMock = {
  allNodes: [
    {hostname: "mickey-mouse.disney.com:8091",
     status: "healthy",
     systemStats: {
       cpu_utilization_rate: 42.5,
       swap_total: 3221225472,
       swap_used: 2969329664
     },
     interestingStats: {
       curr_items: 0,
       curr_items_tot: 0,
       vb_replica_curr_items: 0
     },
     clusterMembership: "inactiveAdded",
     os: 'Linux',
     version: 'only-web.rb',
     uptime: 86400,
     ports: {proxy: 11211,
             direct: 11311},
     memoryTotal: 2032574464,
     memoryFree: 1589864960,
     mcdMemoryReserved: 2032574464,
     mcdMemoryAllocated: 89864960,
     otpNode: "ns1@mickey-mouse.disney.com",
     otpCookie: "SADFDFGDFG"},
    {hostname: "donald-duck.disney.com:8091",
     os: 'Linux',
     uptime: 86420,
     version: 'only-web.rb',
     status: "healthy",
     systemStats: {
       cpu_utilization_rate: 20,
       swap_total: 2547232212,
       swap_used: 1296642969
     },
     interestingStats: {
       curr_items: 1,
       curr_items_tot: 1,
       vb_replica_curr_items: 1
     },
     clusterMembership: "inactiveFailed",
     ports: {proxy: 11211,
             direct: 11311},
     thisNode: true,
     couchApiBase: "/couchBase/",
     memoryTotal: 2032574464,
     memoryFree: 89864960,
     mcdMemoryAllocated: 64,
     mcdMemoryReserved: 256,
     otpNode: "ns1@donald-duck.disney.com",
     otpCookie: "SADFDFGDFG"},
    {hostname: "scrooge-mcduck.disney.com:8091",
     uptime: 865000,
     version: "only-web.rb-2",
     status: "healthy",
     systemStats: {
       cpu_utilization_rate: 20,
       swap_total: 2521247232,
       swap_used: 1296329669
     },
     interestingStats: {
       curr_items: 5,
       curr_items_tot: 11,
       vb_replica_curr_items: 20
     },
     clusterMembership: "active",
     ports: {proxy: 11211,
             direct: 11311},
     couchApiBase: "/couchBase/",
     memoryTotal: 2032574464,
     memoryFree: 89864960,
     mcdMemoryAllocated: 64,
     mcdMemoryReserved: 256,
     otpNode: "ns1@scrooge-mcduck.disney.com",
     otpCookie: "SADFDFGDFG"},
    {hostname: "goofy.disney.com:8091",
     uptime: 86430,
     os: 'Linux',
     version: 'only-web.rb',
     status: "unhealthy",
     systemStats: {
       cpu_utilization_rate: 0.53,
       swap_total: 2547232,
       swap_used: 42969
     },
     interestingStats: {
       curr_items: 5,
       curr_items_tot: 11,
       vb_replica_curr_items: 20
     },
     clusterMembership: "active",
     failedOver: false,
     memoryTotal: 2032574464,
     memoryFree: 889864960,
     mcdMemoryAllocated: 64,
     mcdMemoryReserved: 256,
     ports: {proxy: 11211,
             direct: 11311},
     couchApiBase: "/couchBase/",
     otpNode: "ns1@goofy.disney.com",
     otpCookie: "SADFDFGDFG"}],
  basePoolDetails: {
    "storageTotals": {
      "ram": {
        "quotaUsed": 314572800,
        "usedByData": 3402136,
        "total": 8315748352,
        "quotaTotal": 629145600,
        "used": 6067367936
      },
      "hdd": {
        "usedByData": 0,
        "total": 249064775680,
        "quotaTotal": 249064775680,
        "used": 239102184652,
        "free": 9962591028
      }
    },
    "name": "default",
    "alerts": [],
    "nodes": [],
    "buckets": {
      "uri": "/pools/default/buckets?v=86081550"
    },
    "tasks": {
      "uri": "/pools/default/buckets?v=86081550"
    },
    "remoteClusters": {
      "uri": "/pools/default/remoteClusters",
      "validateURI": "/pools/default/remoteClusters?just_validate=1"
    },
    "controllers": {
      "addNode": {
        "uri": "/controller/addNode"
      },
      "rebalance": {
        "uri": "/controller/rebalance"
      },
      "failOver": {
        "uri": "/controller/failOver"
      },
      "reAddNode": {
        "uri": "/controller/reAddNode"
      },
      "ejectNode": {
        "uri": "/controller/ejectNode"
      },
      "setAutoCompaction": {
        "uri": "/controller/setAutoCompaction",
        "validateURI": "/controller/setAutoCompaction?just_validate=1"
      },
      "replication": {
        "createURI": "/controller/createReplication",
        "infosURI": "/couchBase/my_replicator/_design/_replicator_info/_view/infos?group_level=1"
      }
    },
    "balanced": true,
    "failoverWarnings": [
      "hardNodesNeeded"
    ],
    "alerts": [],
    "rebalanceStatus": "none",
    "rebalanceProgressUri": "/pools/default/rebalanceProgress",
    "stopRebalanceUri": "/controller/stopRebalance",
    "nodeStatusesUri": "/nodeStatuses",
    "stats": {
      "uri": "/pools/default/stats"
    },
    "autoCompactionSettings": {
      "databaseFragmentationThreshold": 99,
      "viewFragmentationThreshold": 98,
      "parallelDBAndViewCompaction": false //,
      // "allowedTimePeriod": {
      //   "fromHour": 11,
      //   "fromMinute": 0,
      //   "toHour": 13,
      //   "toMinute": 0,
      //   "abortOutside": true
      // }
    }
  },
  baseBuckets: [
    {
      "name": "default",
      "bucketType": "membase",
      "authType": "sasl",
      "saslPassword": "",
      "proxyPort": 0,
      "uri": "/pools/default/buckets/default",
      "streamingUri": "/pools/default/bucketsStreaming/default",
      "localRandomKeyUri": "/pools/default/buckets/default/localRandomKey",
      "flushCacheUri": "/pools/default/buckets/default/controller/doFlush",
      "nodes": [],
      "stats": {
        "uri": "/pools/default/buckets/default/stats",
        "directoryURI": "/pools/default/buckets/default/statsDirectory",
        "nodeStatsListURI": "/pools/default/buckets/default/nodes"
      },
      "nodeLocator": "vbucket",
      "vBucketServerMap": {
        "hashAlgorithm": "CRC",
        "numReplicas": 1,
        "serverList": [], // not used
        "vBucketMap": [] // node used
      },
      "replicaNumber": 1,
      "quota": {
        "ram": 314572800,
        "rawRAM": 314572800
      },
      "basicStats": {
        "quotaPercentUsed": 1.0815099080403645,
        "opsPerSec": 0,
        "diskFetches": 0,
        "itemCount": 0,
        "diskUsed": 0,
        "memUsed": 3402136
      },
      "autoCompactionSettings": {
        "databaseFragmentationThreshold": 96,
        "viewFragmentationThreshold": 70,
        "parallelDBAndViewCompaction": true,
        "allowedTimePeriod": {
          "fromHour": 11,
          "fromMinute": 0,
          "toHour": 13,
          "toMinute": 0,
          "abortOutside": true
        }
      },
      "bucketCapabilitiesVer": "sync-1.0",
      "bucketCapabilities": ["touch","sync","couchapi"]
    },
    {
      "name": "mcd",
      "bucketType": "memcached",
      "authType": "sasl",
      "saslPassword": "",
      "proxyPort": 0,
      "uri": "/pools/default/buckets/mcd",
      "streamingUri": "/pools/default/bucketsStreaming/mcd",
      "flushCacheUri": "/pools/default/buckets/mcd/controller/doFlush",
      "nodes": [],
      "stats": {
        "uri": "/pools/default/buckets/mcd/stats",
        "directoryURI": "/pools/default/buckets/mcd/statsDirectory",
        "nodeStatsListURI": "/pools/default/buckets/mcd/nodes"
      },
      "nodeLocator": "ketama",
      "replicaNumber": 0,
      "quota": {
        "ram": 314572800,
        "rawRAM": 314572800
      },
      "basicStats": {
        "quotaPercentUsed": 0.0,
        "opsPerSec": 0,
        "hitRatio": 0,
        "itemCount": 0,
        "diskUsed": 0,
        "memUsed": 0
      },
      "bucketCapabilitiesVer": "sync-1.0",
      "bucketCapabilities": []
    }
  ],
  pools: function (req) {
    var rv = {
      "pools": [
        {
          "name": "default",
          "uri": "/pools/default",
          "streamingUri": "/poolsStreaming/default"
        }
      ],
      "isAdminCreds": !!req.fakeXHR.login,
      "uuid": "6f0abb80-6aa8-4001-15e8-97aa00000226",
      "implementationVersion": "1.7.0_207_gcddb6e2",
      "componentsVersion": {
        "public_key": "0.12",
        "os_mon": "2.2.6",
        "mnesia": "4.4.19",
        "inets": "5.6",
        "couch": "1.2.0a-930e7c7-git",
        "kernel": "2.14.4",
        "crypto": "2.0.3",
        "ssl": "4.1.5",
        "sasl": "2.1.9.4",
        "ns_server": "1.7.0_207_gcddb6e2",
        "mochiweb": "1.4.1",
        "ibrowse": "2.2.0",
        "oauth": "7d85d3ef",
        "stdlib": "1.17.4"
      }
    };
    if (__hookParams['forceWiz']) {
      rv.pools.length = 0;
    }
    return rv;
  },
  poolDetails: function () {
    var rv = _.clone(this.basePoolDetails);
    rv.nodes = _.clone(this.allNodes);
    rv.etag = "asdasd";
    return rv;
  },
  nodeStatuses: function () {
    return _.map(this.allNodes, function (ninfo) {
      return {status: ninfo.status,
              otpNode: ninfo.otpNode,
              replication: 1};
    });
  },
  bucketsList: function () {
    var self = this;
    var rv = _.clone(self.baseBuckets);
    _.each(rv, function (ninfo) {ninfo.nodes = _.clone(self.allNodes)});
    return rv;
  },
  bucketDetails: function (path) {
    path = "/" + path.join("/");
    var rv = _.detect(this.baseBuckets, function (binfo) {
      return binfo.uri === path;
    });
    if (!rv) {
      BUG();
    }
    rv.nodes = _.clone(this.allNodes);
    return rv;
  },
  handleBucketRemoval: function (options) {
  },
  handleBucketsPost: function (params) {
    var errors = {};

    // if (isBlank(params['name'])) {
    //   errors.name = 'name cannot be blank';
    // } else if (params['name'] != 'new-name') {
    //   errors.name = 'name has already been taken';
    // }

    // if (!(/^\d+$/.exec(params['ramQuotaMB']))) {
    //   errors.ramQuotaMB = "RAM quota size must be an integer";
    // }

    // if (!(/^\d+$/.exec(params['hddQuotaGB']))) {
    //   errors.hddQuotaGB = "Disk quota size must be an integer";
    // }

    // var authType = params['authType'];
    // if (authType == 'none') {
    //   if (!(/^\d+$/.exec(params['proxyPort']))) {
    //     errors.proxyPort = 'bad'
    //   }
    // } else if (authType == 'sasl') {
    // } else {
    //   errors.authType = 'unknown auth type'
    // }

    // if (_.keys(errors).length) {
    //   return this.errorResponse(errors);
    // }

    var rv = {"errors":{},"summaries":{"ramSummary":{"total":629145600,"otherBuckets":0,"nodesCount":1,"perNodeMegs":300,"thisAlloc":314572800,"thisUsed":0,"free":314572800},"hddSummary":{"total":249064775680,"otherData":239102184652,"otherBuckets":0,"thisUsed":0,"free":9962591028}}}

    return rv;
  },
  handleCreateReplication: function (req) {
    var args = req.deserialize();

    return function () {
      if (__hookParams['validateReplicationWorks'] == 'false') {
        req.errorResponse({bucketFrom: "source bucket is invalid"});
        return;
      }

      var errors = {};
      var hadError = false;
      _.each(("fromBucket toBucket replicationType toCluster").split(" "), function (field) {
        if (args[field] == null || args[field] === '') {
          hadError = true;
          errors[field] = field + ' cannot be empty';
        }
      });
      if (hadError) {
        req.errorResponse(errors);
      } else {
        req.fakeResponse("");
      }
    }
  },
  handleAllDocs: function () {
    var rv = {"total_rows":20,"offset":12,"rows":[
      { "id":"_design/adhoc",
        "key":"_design/adhoc",
        "value":{"rev":"13-ea509c85ed1fe2102632de72b5de4043"},
        "doc":{
          "_id":"_design/adhoc",
          "_rev":"13-ea509c85ed1fe2102632de72b5de4043",
          "language":"javascript",
          "views":{
            "query":{
              "map":"function(doc, meta) {\n  for (var k in doc) {\n    if (k.substr(0,1) !== '_') {\n      emit(doc[k], {\"_id\": meta.id, \"path\": k});\n    }\n  }\n}"
            }
          }
        }
      },
      { "id":"_design/dev_TeaTime",
        "key":"_design/dev_TeaTime",
        "value":{"rev":"1-5ade2e707ee6f3b2d4467ff5e8f2a96a"},
        "doc":{
          "_id":"_design/dev_TeaTime",
          "_rev":"1-5ade2e707ee6f3b2d4467ff5e8f2a96a",
          "language": "javascript",
          "views":{
            "all_users":{
              "map":"function(doc) {\n  function emitUsers(users) {\n    if (users && users.forEach) {\n      users.forEach(function(user) {\n        emit(user, 1);\n      });\n    }\n  }\n  if (doc.type == \"event\") {\n        emitUsers(doc.attendees);\n        emitUsers(doc.hosts);\n  }\n  if (doc.type == \"profile\") {\n        emitUsers(doc, doc.attendees, doc.start && doc.end);\n  }\n};",
              "reduce":"_count"
            },
            "by_user_date":{
              "map":"function(doc) {\n  function emitUsersMonthly(doc, users, start, end) {\n    if (users && users.forEach) {\n      users.forEach(function(user) {\n        emitUserMonthly(doc, user, doc.start, doc.end);\n      });\n    }\n  }\n  function emitUserMonthly(doc, user, start, end) {\n    var totalEmitted = 0;\n    while (totalEmitted++ < 36 && start <= end) {\n      var d = new Date(start);\n      emit([user, d.getFullYear(), d.getMonth()], doc);\n      if (d.getMonth() == 11) {\n        d.setFullYear(d.getFullYear() + 1);\n        d.setMonth(0);\n      } else {\n        d.setMonth(d.getMonth() + 1);\n      }\n      start = d.getTime();\n    }\n  }\n  if (doc.type == \"event\" && doc.start && doc.end\n      && typeof doc.start == \"number\" && typeof doc.end == \"number\") {\n        emitUsersMonthly(doc, doc.attendees, doc.start && doc.end);\n        emitUsersMonthly(doc, doc.hosts, doc.start && doc.end);\n  }\n};"
            }
          }
        }
      },
      { "id":"_design/TeaTime",
        "key":"_design/TeaTime",
        "value":{"rev":"1-5ade2e707ee6f3b2d4467ff5e8f2a96a"},
        "doc":{
          "_id":"_design/TeaTime",
          "_rev":"1-5ade2e707ee6f3b2d4467ff5e8f2a96a",
          "language": "javascript",
          "views":{
            "all_users":{
              "map":"function(doc) {\n  function emitUsers(users) {\n    if (users && users.forEach) {\n      users.forEach(function(user) {\n        emit(user, 1);\n      });\n    }\n  }\n  if (doc.type == \"event\") {\n        emitUsers(doc.attendees);\n        emitUsers(doc.hosts);\n  }\n  if (doc.type == \"profile\") {\n        emitUsers(doc, doc.attendees, doc.start && doc.end);\n  }\n};",
              "reduce":"_count"
            },
            "by_user_date":{
              "map":"function(doc) {\n  function emitUsersMonthly(doc, users, start, end) {\n    if (users && users.forEach) {\n      users.forEach(function(user) {\n        emitUserMonthly(doc, user, doc.start, doc.end);\n      });\n    }\n  }\n  function emitUserMonthly(doc, user, start, end) {\n    var totalEmitted = 0;\n    while (totalEmitted++ < 36 && start <= end) {\n      var d = new Date(start);\n      emit([user, d.getFullYear(), d.getMonth()], doc);\n      if (d.getMonth() == 11) {\n        d.setFullYear(d.getFullYear() + 1);\n        d.setMonth(0);\n      } else {\n        d.setMonth(d.getMonth() + 1);\n      }\n      start = d.getTime();\n    }\n  }\n  if (doc.type == \"event\" && doc.start && doc.end\n      && typeof doc.start == \"number\" && typeof doc.end == \"number\") {\n        emitUsersMonthly(doc, doc.attendees, doc.start && doc.end);\n        emitUsersMonthly(doc, doc.hosts, doc.start && doc.end);\n  }\n};"
            }
          }
        }
      }
    ]};
    return rv;
  },
  handleAnyDoc: function () {
    var all = [{
      "_id": "234235124",
      "_rev": "3-12342341234",
      "type": "event",
      "title": "meeting",
      "where": "coffee bar",
      "hosts": [
        "benjamin@couchbase.com"
      ]
    }, {
      "_id": "0594680c9ba809979a8e5f9a8000027c",
      "_rev": "3-7f43cdfce1b537739736a97c4eb78d62",
      "created_at": "2010-07-04T18:06:34.020Z",
      "profile": {
        "rand": "0.8309284392744303",
        "email": "jchris@couch.io",
        "url": "http://jchrisa.net",
        "gravatar_url": "http://www.gravatar.com/avatar/6f09a637f926f04d9b34bfe10e94bd3e.jpg?s=40&d=identicon",
        "name": "jchris"
      },
      "message": "refactor #focus evently.nav so it is properly bound to login and logout status",
      "state": "done",
      "publish": true,
      "type": "task",
      "edit_at": "2010-07-11T22:08:52.928Z",
      "edit_by": "jchris"
    }, {
      "_id": "1643684c68d03fb70bc98d88a8896d0d",
      "_rev": "6-ff271e27fd0edfd88afb1d16e1363f79",
      "urls": {
        "@doppler": "http://twitter.com/doppler",
        "Web Developer, SXSW": "http://sxsw.com",
        "github.com/doppler": "http://github.com/doppler"
      },
      "bio": "I've been playing with and using CouchDB since version 0.9.0, sometime in early 2009. The first real app I created on CouchDB was to solve the problem of needing to provide an API to query SXSW schedule data, to be used by a 3rd-party developer creating an iPhone app. I'm hoping to build on that idea for SXSW 2011.",
      "hometown": "Austin, TX",
      "name": "David Rose",
      "_attachments": {
        "Photo on 2010-09-10 at 14.44.jpg": {
          "content_type": "image/jpeg",
          "revpos": 5,
          "digest": "md5-dlyF/44110seO+xxDgrkHA==",
          "length": 79027,
          "stub": true
        }
      }
    }];
    return all[(Math.random() * all.length) >> 0];
  },
  handleReplicatorInfos: function (req) {
    return {
      "rows": [
        {
          "key": [
            "missing-rep_2"
          ],
          "value": {
            "_replication_state": "completed",
            "_replication_state_time": "2011-10-18T15:49:45Z",
            "_replication_fields": {
              "_id": "missing-rep_2",
              "source": "default",
              "target": "/remoteClusters/other.local/buckets/other-bucket",
              "continuous": false
            },
            "have_replicator_doc": false,
            "count": 2
          }
        },
        {
          "key": [
            "rep_1"
          ],
          "value": {
            "_replication_state": "triggered",
            "_replication_state_time": "2011-10-18T15:52:44Z",
            "_replication_fields": {
              "_id": "rep_1",
              "source": "default",
              "target": "/remoteClusters/kyiv/buckets/default",
              "continuous": false
            },
            "have_replicator_doc": true,
            "count": 2
          }
        },
        {
          "key": [
            "rep_2"
          ],
          "value": {
            "_replication_state": "completed",
            "_replication_state_time": "2011-10-18T15:51:44Z",
            "_replication_fields": {
              "_id": "rep_2",
              "source": "other-bucket",
              "target": "/remoteClusters/london/buckets/very-other-bucket",
              "continuous": true
            },
            "have_replicator_doc": true,
            "count": 2
          }
        }
      ]
    }

  }
};

TestingSupervisor.interceptAjax();

var __hookParams = {};

(function () {
  var href = window.location.href;
  var match = /\?(.*?)(?:$|#)/.exec(href);
  if (!match)
    return;
  var params = __hookParams = deserializeQueryString(match[1]);

  console.log("params", params);

  if (params['auth'] == '1')
    MockedRequest.prototype.checkAuth = MockedRequest.prototype.checkAuthReal;

  if (params['ajaxDelay']) {
    ajaxRespondDelay = parseInt(params['ajaxDelay'], 10);
  }

  if (params['nowiz']) {
    DAL.login = 'Administrator'
    DAL.password = 'asdasd';
  }

  if (params['single']) {
    ServerStateMock.allNodes = ServerStateMock.allNodes.slice(-1);
  }

  if (params['healthy']) {
    _.each(ServerStateMock.allNodes, function (ninfo) {
      ninfo.status = "healthy";
    });
  }

  if (params['rebalanceStatus']) {
    MockedRequest.globalData.setRebalanceStatus(params['rebalanceStatus']);
  }

  if (params['dialog']) {
    $(function () {
      $($i(params['dialog'])).show();
    });
  }
})();

//window.onerror = originalOnError;
