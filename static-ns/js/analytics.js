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
var StatsModel = {};

(function (self) {
  // starts future.get with given ajaxOptions and arranges invocation
  // of 'body' with value or if future is cancelled invocation of body
  // with 'cancelledValue'.
  //
  // NOTE: this is not normal future as normal futures deliver values
  // to cell, but this one 'delivers' values to given function 'body'.
  function getCPS(ajaxOptions, cancelledValue, body) {
    // this wrapper calls 'body' instead of delivering value to it's
    // cell
    var futureWrapper = future.wrap(function (ignoredDataCallback, startGet) {
      return startGet(body);
    });
    var async = future.get(ajaxOptions, undefined, undefined, futureWrapper);
    async.cancel = (function (realCancel) {
      return function () {
        if (realCancel) {
          realCancel.call(this);
        }
        body(cancelledValue);
      }
    })(async.cancel);
    // we need any cell here to satisfy our not yet perfect API
    async.start(new Cell());
    return async;
  }

  function createSamplesFuture(statsURL, statsData, bufferDepth, realTimeRestorer) {
    var cancelMark = {};
    var mark404 = {};

    function doGet(data, body) {
      if (mainAsync.cancelled) {
        return body(cancelMark);
      }

      function onCancel() {
        async.cancel();
      }
      function unbind() {
        $(mainAsync).unbind('cancelled', onCancel);
      }
      $(mainAsync).bind('cancelled', onCancel);

      var async;
      return async = getCPS({url: statsURL, data: data, missingValue: mark404}, cancelMark, function (value, status, xhr) {
        unbind();
        if (value !== cancelMark && value !== mark404) {
          var date = xhr.getResponseHeader('date');
          value.serverDate = parseHTTPDate(date).valueOf();
          value.clientDate = (new Date()).valueOf();
        }
        body(value, status, xhr);
      });
    }

    var mainAsync = future(function (dataCallback) {
      function deliverValue(value) {
        return dataCallback.continuing(value);
      }
      nonRealtimeLoop(deliverValue);
    });
    mainAsync.cancel = function () {
      $(this).trigger('cancelled');
    }

    return mainAsync;

    function nonRealtimeLoop(deliverValue) {
      doGet(statsData, function (value) {
        if (value === cancelMark || value === mark404) {
          return;
        }

        deliverValue(value);

        // this is the only non-generic place in this function. We're
        // able to extract interval from specific and from normal
        // stats
        var interval = value.interval || value.op.interval;
        if (interval < 2000) {
          startRealTimeLoop(deliverValue, value);
        } else {
          setTimeout(function () {
            nonRealtimeLoop(deliverValue);
          }, Math.min(interval/2, 60000));
        }
      });
    }

    function startRealTimeLoop(realDeliverValue, value) {
      // we're going to actually deliver values each second and
      // deliverValue we pass to loop just stores last value
      function deliverValue(val) {
        value = val;
      }
      var intervalId = setInterval(function () {
        realDeliverValue(_.clone(value));
      }, 1000);

      function onCancel() {
        clearInterval(intervalId);
        unbind();
      }
      function unbind() {
        $(mainAsync).unbind('cancelled', onCancel);
      }
      $(mainAsync).bind('cancelled', onCancel);

      realTimeLoop(deliverValue, value, realTimeRestorer);
    }

    function realTimeLoop(deliverValue, lastValue, realTimeRestorer) {
      realTimeRestorer(lastValue, statsData, bufferDepth, function (data, restorer) {
        doGet(data, function (rawStats) {
          if (rawStats === cancelMark || rawStats === mark404) {
            return;
          }

          var restoredValue = restorer(rawStats);
          deliverValue(restoredValue);
          realTimeLoop(deliverValue, restoredValue, realTimeRestorer);
        });
      });
    }
  }

  // Prepares data for stats request and returns it and stats
  // restoring function. This code is calling continuation 'cont' with
  // two arguments rather then returning array with two elements (and
  // then having to unpack at call-site).
  //
  // Returned stats restoring function will then be called with result
  // of get request and is responsible for building complete stats
  // from old sample it has and new result.
  function aggregateRealTimeRestorer(lastValue, statsData, bufferDepth, mvReturn) {
    var keepCount = lastValue.op.samplesCount + bufferDepth;
    var data = statsData;

    if (lastValue.op.lastTStamp) {
      data = _.extend({haveTStamp: lastValue.op.lastTStamp}, data);
    } else {
      // if we don't have any stats, pause a bit, dont send new stats
      // request immediately. Because doing so will return immediately
      // making us send another request and so on, spamming server.
      setTimeout(function () {
        mvReturn(data, restorer);
      }, 1000);
      return;
    }

    return mvReturn(data, restorer);
    function restorer(rawStats) {
      var op = rawStats.op;

      var samples = op.samples;
      // if we have empty stats re-use previous value instead of rendering empty stats
      if (samples && samples.timestamp.length === 0) {
        return lastValue;
      }

      if (!op.tstampParam) {
        return rawStats;
      }

      var newSamples = {};
      var prevSamples = lastValue.op.samples;
      for (var keyName in samples) {
        newSamples[keyName] = prevSamples[keyName].concat(samples[keyName].slice(1)).slice(-keepCount);
      }

      var restored = _.clone(rawStats);
      restored.op = _.clone(op);
      restored.op.samples = newSamples;

      return restored;
    }
  }

  function specificStatsRealTimeRestorer(lastValue, statsData, bufferDepth, mvReturn) {
    var keepCount = lastValue.samplesCount + bufferDepth;
    var data = statsData;

    if (lastValue.lastTStamp) {
      data = _.extend({haveTStamp: lastValue.lastTStamp}, data);
    } else {
      // if we don't have any stats, pause a bit, dont send new stats
      // request immediately. Because doing so will return immediately
      // making us send another request and so on, spamming server.
      setTimeout(function () {
        mvReturn(data, restorer);
      }, 1000);
      return;
    }

    return mvReturn(data, restorer);
    function restorer(rawStats) {
      // if we have empty stats re-use previous value instead of rendering empty stats
      if (rawStats.timestamp.length === 0) {
        return lastValue;
      }

      if (!rawStats.tstampParam) {
        return rawStats;
      }

      var samples = rawStats.nodeStats;

      var newSamples = {};
      var prevSamples = lastValue.nodeStats;
      for (var keyName in samples) {
        newSamples[keyName] = prevSamples[keyName].concat(samples[keyName].slice(1)).slice(-keepCount);
      }

      var newTimestamp = lastValue.timestamp.concat(rawStats.timestamp.slice(1)).slice(-keepCount);

      var restored = _.clone(rawStats);
      restored.nodeStats = newSamples;
      restored.timestamp = newTimestamp;

      return restored;
    }
  }

  var statsBucketURL = self.statsBucketURL = new StringHashFragmentCell("statsBucket");
  var statsHostname = self.statsHostname = new StringHashFragmentCell("statsHostname");
  var statsStatName = self.statsStatName = new StringHashFragmentCell("statsStatName");

  // contains bucket details of statsBucketURL bucket (or default if there are no such bucket)
  var statsBucketDetails = self.statsBucketDetails = Cell.compute(function (v) {
    var uri = v(statsBucketURL);
    var buckets = v.need(DAL.cells.bucketsListCell);
    var rv;
    if (uri !== undefined) {
      rv = _.detect(buckets, function (info) {return info.uri === uri});
    } else {
      rv = _.detect(buckets, function (info) {return info.name === "default"}) || buckets[0];
    }
    return rv;
  }).name("statsBucketDetails");

  self.knownHostnamesCell = Cell.compute(function (v) {
    var allNames = _.pluck(v.need(DAL.cells.serversCell).allNodes, 'hostname').sort();
    var activeNames = _.pluck(v.need(DAL.cells.serversCell).active, 'hostname').sort();
    return {all: allNames, active: activeNames};
  }).name("knownHostnamesCell");
  self.knownHostnamesCell.equality = _.isEqual;

  // contains list of links to per-node stats for particular bucket
  var statsNodesCell = self.statsNodesCell = Cell.compute(function (v) {
    v.need(self.knownHostnamesCell); // this cell should be refreshed
                                     // when list of known nodes
                                     // changes
    return future.get({url: v.need(statsBucketDetails).stats.nodeStatsListURI});
  }).name("statsNodesCell");

  var statsOptionsCell = self.statsOptionsCell = (new Cell()).name("statsOptionsCell");
  statsOptionsCell.setValue({nonQ: ['keysInterval', 'nonQ'], resampleForUI: '1'});
  _.extend(statsOptionsCell, {
    update: function (options) {
      this.modifyValue(_.bind($.extend, $, {}), options);
    },
    equality: _.isEqual
  });

  var samplesBufferDepthRAW = new StringHashFragmentCell("statsBufferDepth");
  self.samplesBufferDepth = Cell.computeEager(function (v) {
    return v(samplesBufferDepthRAW) || 1;
  });

  var statsDirectoryURLCell = self.statsDirectoryURLCell = Cell.compute(function (v) {
    if (v.need(DAL.cells.mode) !== 'analytics') {
      return;
    }
    return v.need(statsBucketDetails).stats.directoryURI;
  }).name("statsDirectoryURLCell");

  var rawStatsDescCell = self.rawStatsDescCell = Cell.compute(function (v) {
    return future.get({url: v.need(statsDirectoryURLCell)});
  }).name("rawStatsDescCell");

  var nameToStatInfoCell = Cell.compute(function (v) {
    var desc = v.need(rawStatsDescCell);
    var allStatsInfos = [].concat.apply([], _.pluck(desc.blocks, 'stats'));
    var rv = {};
    _.each(allStatsInfos, function (info) {
      rv[info.name] = info;
    });
    return rv;
  }).name("nameToStatInfoCell");

  var specificStatsInfo = Cell.compute(function (v) {
    var statName = v(statsStatName);
    if (!statName) {
      return {url: null, statName: null, desc: null};
    }
    var mapping = v.need(nameToStatInfoCell);
    if (!mapping[statName]) {
      return {url: null, statName: null, desc: null};
    }
    return {url: mapping[statName].specificStatsURL,
            statName: statName, desc: mapping[statName].desc};
  }).name("specificStatsInfo");

  var effectiveSpecificStatName = self.effectiveSpecificStatName = Cell.compute(function (v) {
    return v.need(specificStatsInfo).statName;
  }).name("effectiveSpecificStatName");
  effectiveSpecificStatName.equality = function (a,b) {return a===b};

  var specificStatsURLCell = self.specificStatsURLCell = Cell.compute(function (v) {
    return v.need(specificStatsInfo).url;
  }).name("specificStatsURLCell");
  specificStatsURLCell.equality = function (a,b) {return a===b};

  var specificStatDescriptionCell = self.specificStatDescriptionCell = Cell.compute(function (v) {
    return v.need(specificStatsInfo).desc;
  }).name("specificStatDescriptionCell");
  specificStatDescriptionCell.equality = function (a,b) {return a===b};

  // true if we should be displaying specific stats and false if we should be displaying normal stats
  var displayingSpecificStatsCell = self.displayingSpecificStatsCell = Cell.compute(function (v) {
    return !!v.need(specificStatsURLCell);
  }).name("displayingSpecificStatsCell");

  // contains either bucket info or per-node stat info (as found in
  // statsNodesCell response) for which (normal) stats are displayed
  var targetCell = self.targetCell = Cell.compute(function (v) {
    if (v.need(DAL.cells.mode) !== 'analytics' || v.need(displayingSpecificStatsCell)) {
      return;
    }

    var hostname = v(statsHostname);
    if (!hostname) {
      return v.need(statsBucketDetails);
    }

    var nodes = v.need(statsNodesCell);
    var nodeInfo = _.detect(nodes.servers, function (info) {return info.hostname === hostname});
    if (!nodeInfo) {
      return v.need(statsBucketDetails);
    }

    return nodeInfo;
  }).name("targetCell");

  var statsURLCell = self.statsURLCell = Cell.compute(function (v) {
    return v.need(targetCell).stats.uri;
  }).name("statsURLCell");

  var zoomLevel;
  (function () {
    var slider = $("#date_slider").slider({
      orientation: "vertical",
      range: "min",
      min: 1,
      max: 6,
      value: 6,
      step: 1,
      slide: function(event, ui) {
        $("#date_slider_container ul li:nth-child(" + (7 - ui.value) + ") a")
          .trigger('click');
      }
    });

    zoomLevel = (new LinkSwitchCell('zoom', {
      firstItemIsDefault: true
    })).name("zoomLevel");

    _.each('minute hour day week month year'.split(' '), function (name) {
      zoomLevel.addItem('zoom_' + name, name)
    });

    zoomLevel.finalizeBuilding();

    zoomLevel.subscribeValue(function (zoomLevel) {
      var z = $('#zoom_' + zoomLevel);
      z.css('font-weight', 'bold');
      z.parent().siblings().find('a').css('font-weight', 'normal');
      slider.slider('value', 6 - z.parent().index());
      self.statsOptionsCell.update({
        zoom: zoomLevel
      });
    });
  })();

  self.zoomLevel = zoomLevel;

  var statsCell = self.statsCell = Cell.compute(function (v) {
    if (v.need(DAL.cells.mode) != 'analytics') {
      return;
    }
    var options = v.need(statsOptionsCell);
    var url = v.need(statsURLCell);

    var data = _.extend({}, options);
    _.each(data.nonQ, function (n) {
      delete data[n];
    });

    var bufferDepth = v.need(self.samplesBufferDepth);

    return createSamplesFuture(url, data, bufferDepth, aggregateRealTimeRestorer);
  }).name("statsCell");

  var specificStatsCell = self.specificStatsCell = Cell.compute(function (v) {
    if (v.need(DAL.cells.mode) != 'analytics') {
      return;
    }

    var url = v(specificStatsURLCell);
    if (!url) { // we're 'expecting' null here too
      return;
    }
    var options = v.need(statsOptionsCell);

    var data = _.extend({}, options);
    _.each(data.nonQ, function (n) {
      delete data[n];
    });

    var bufferDepth = v.need(self.samplesBufferDepth);

    return createSamplesFuture(url, data, bufferDepth, specificStatsRealTimeRestorer);
  }).name("specificStatsCell");

  // containts list of hostnames for specific stats ordered by natural
  // sort order of hostname or ip
  var specificStatsNodesCell = Cell.compute(function (v) {
    return _.keys(v.need(specificStatsCell).nodeStats).sort(naturalSort);
  }).name("specificStatsNodesCell");

  var visibleStatsDescCell = self.visibleStatsDescCell = Cell.compute(function (v) {
    if (v.need(displayingSpecificStatsCell)) {
      var nodes = v.need(specificStatsNodesCell);
      var statsDesc = v.need(specificStatDescriptionCell);
      return {thisISSpecificStats: true,
              blocks: [{blockName: "Specific Stats", hideThis: true,
                        stats: _.map(nodes, function (hostname) {
                          return {title: ViewHelpers.maybeStripPort(hostname, nodes),
                                  name: hostname, desc: statsDesc};
                        })}]};
    } else {
      return v.need(rawStatsDescCell);
    }
  }).name("visibleStatsDescCell");

  self.infosCell = Cell.needing(visibleStatsDescCell).compute(function (v, statDesc) {
    statDesc = JSON.parse(JSON.stringify(statDesc)); // this makes deep copy of statDesc

    var infos = [];
    infos.byName = {};

    var statItems = [];
    var blockIDs = [];

    var hadServerResources = false;
    if (statDesc.blocks[0].serverResources) {
      // We want it last so that default stat name (which is first
      // statItems entry) is not from ServerResourcesBlock
      hadServerResources = true;
      statDesc.blocks = statDesc.blocks.slice(1).concat([statDesc.blocks[0]]);
    }
    _.each(statDesc.blocks, function (aBlock) {
      var blockName = aBlock.blockName;
      aBlock.id = _.uniqueId("GB");
      blockIDs.push(aBlock.id);
      var stats = aBlock.stats;
      statItems = statItems.concat(stats);
      _.each(stats, function (statInfo) {
        statInfo.id = _.uniqueId("G");
        statInfo.blockId = aBlock.id;
      });
    });
    // and now make ServerResourcesBlock first for rendering
    if (hadServerResources) {
      statDesc.blocks.unshift(statDesc.blocks.pop());
    }
    _.each(statItems, function (item) {
      infos.push(item);
      infos.byName[item.name] = item;
    });
    infos.blockIDs = blockIDs;

    return {statDesc: statDesc, infos: infos};
  }).name("infosCell");

  self.statsDescInfoCell = Cell.needing(self.infosCell).compute(function (v, infos) {
    return infos.statDesc;
  }).name("statsDescInfoCell");

  self.serverResourcesVisible = Cell.compute(function (v) {
    // server resources are visible iff infos cell is not undefined
    // and statsHostname is not blank
    return !!v(self.infosCell) && !!v(self.statsHostname);
  }).name("serverResourcesVisible");

  self.selectedGraphNameCell = (new StringHashFragmentCell("graph")).name("selectedGraphNameCell");

  self.configurationExtra = (new Cell()).name("configurationExtra");

  self.smallGraphSelectionCellCell = Cell.compute(function (v) {
    return v.need(displayingSpecificStatsCell) ? self.statsHostname : self.selectedGraphNameCell;
  }).name("smallGraphSelectionCellCell");

  self.aggregateGraphsConfigurationCell = Cell.compute(function (v) {
    var selectedGraphName = v(self.selectedGraphNameCell);
    var stats = v.need(self.statsCell);
    var selected;

    var infos = v.need(self.infosCell).infos;
    if (!selectedGraphName || !(selectedGraphName in infos.byName)) {
      selected = infos[0];
    } else {
      selected = infos.byName[selectedGraphName];
    }

    var op = stats.op;
    if (!op) {
      BUG();
    }

    if (!op.samples[selected.name]) {
      selected = _.detect(infos, function (info) {return op.samples[info.name];}) || selected;
    }

    return {
      interval: op.interval,
      zoomLevel: v.need(zoomLevel),
      selected: selected,
      samples: op.samples,
      timestamp: op.samples.timestamp,
      serverDate: stats.serverDate,
      clientDate: stats.clientDate,
      infos: infos,
      extra: v(self.configurationExtra)
    };
  }).name("aggregateGraphsConfigurationCell");

  self.specificGraphsConfigurationCell = Cell.compute(function (v) {
    var infos = v.need(self.infosCell).infos;
    var stats = v.need(specificStatsCell);
    var selectedHostname = v(statsHostname);
    var selected;

    var infos = v.need(self.infosCell).infos;
    if (!selectedHostname || !(selectedHostname in infos.byName)) {
      selected = infos[0];
    } else {
      selected = infos.byName[selectedHostname];
    }

    if (!stats.nodeStats[selected.name]) {
      selected = _.detect(infos, function (info) {return stats.nodeStats[info.name];}) || selected;
    }

    return {
      interval: stats.interval,
      zoomLevel: v.need(zoomLevel),
      selected: selected,
      samples: stats.nodeStats,
      timestamp: stats.timestamp,
      serverDate: stats.serverDate,
      clientDate: stats.clientDate,
      infos: infos,
      extra: v(self.configurationExtra)
    };
  }).name("specificGraphsConfigurationCell");

  self.specificStatsNamesSetCell = Cell.compute(function (v) {
    var rawDesc = v.need(rawStatsDescCell);
    var knownDescTexts = {};
    var allStatInfos = [].concat.apply([], _.pluck(rawDesc.blocks, 'stats'));

    _.each(allStatInfos, function (statInfo) {
      if (statInfo.missing) {
        return;
      }
      knownDescTexts[statInfo.title] = (knownDescTexts[statInfo.title] || 0) + 1;
    });

    var markedNames = {};
    var result = [];
    _.each(rawDesc.blocks, function (blockInfo) {
      _.each(blockInfo.stats, function (statInfo, idx) {
        var title = statInfo.title;
        if (!title) {
          throw new Error();
        }
        if (knownDescTexts[title] > 1 && blockInfo.columns) {
          // we have 'shared name'. So let's 'unshare' it be prepending column name
          title = blockInfo.columns[idx % 4] + ' ' + title;
        }
        var name = statInfo.name;
        if (markedNames[name]) {
          return;
        }
        result.push([name, title]);
        markedNames[name] = true;
      });
    });

    result = _.sortBy(result, function (r) {return r[1]});

    var byName = result.byName = {};
    _.each(result, function (pair) {
      byName[pair[0]] = pair;
    });

    return result;
  }).name("specificStatsNamesSetCell");

  self.graphsConfigurationCell = Cell.compute(function (v) {
    if (v.need(self.displayingSpecificStatsCell)) {
      return v.need(self.specificGraphsConfigurationCell);
    } else {
      return v.need(self.aggregateGraphsConfigurationCell);
    }
  }).name("graphsConfigurationCell");

  self.hotKeysCell = Cell.compute(function (v) {
    if (v.need(displayingSpecificStatsCell)) {
      return null;
    }
    return v.need(statsCell).hot_keys;
  }).name("hotKeysCell");
  self.hotKeysCell.equality = function (a,b) {return a===b;};

})(StatsModel);

var maybeReloadAppDueToLeak = (function () {
  var counter = 300;

  return function () {
    if (!window.G_vmlCanvasManager)
      return;

    if (!--counter)
      reloadPage();
  };
})();


;(function (global) {

  var queuedUpdates = [];

  function flushQueuedUpdate() {
    var i = queuedUpdates.length;
    while (--i >= 0) {
      queuedUpdates[i]();
    }
    queuedUpdates.length = 0;
  }

  var shadowSize = 3;

  if (window.G_vmlCanvasManager) {
    shadowSize = 0;
  }

  function renderSmallGraph(jq, options) {
    function reqOpt(name) {
      var rv = options[name];
      if (rv === undefined)
        throw new Error("missing option: " + name);
      return rv;
    }
    var data = reqOpt('data');
    var now = reqOpt('now');
    var plotSeries = buildPlotSeries(data,
                                     reqOpt('timestamp'),
                                     reqOpt('breakInterval'),
                                     reqOpt('timeOffset')).plotSeries;

    var lastY = data[data.length-1];

    var maxString = isNaN(lastY) ? '?' : ViewHelpers.formatQuantity(lastY, '', 1000);
    queuedUpdates.push(function () {
      jq.find('.small_graph_label > .value').text(maxString);
    });
    if (queuedUpdates.length == 1) {
      setTimeout(flushQueuedUpdate, 0);
    }

    var color = reqOpt('isSelected') ? '#e2f1f9' : '#d95e28';

    var yaxis = {min:0, ticks:0, autoscaleMargin: 0.04}

    if (options.maxY)
      yaxis.max = options.maxY;

    $.plot(jq.find('.small_graph_block'),
           _.map(plotSeries, function (plotData) {
             return {color: color,
                     shadowSize: shadowSize,
                     data: plotData};
           }),
           {xaxis: {ticks:0,
                    autoscaleMargin: 0.04,
                    min: now - reqOpt('zoomMillis'),
                    max: now},
            yaxis: yaxis,
            grid: {show:false}});
  }

  global.renderSmallGraph = renderSmallGraph;
})(this);

var GraphsWidget = mkClass({
  initialize: function (largeGraphJQ, smallGraphsContainerJQ, descCell, configurationCell) {
    this.largeGraphJQ = largeGraphJQ;
    this.smallGraphsContainerJQ = smallGraphsContainerJQ;

    this.drawnDesc = this.drawnConfiguration = {};
    Cell.subscribeMultipleValues($m(this, 'renderAll'), descCell, configurationCell);
  },
  // renderAll (and subscribeMultipleValues) exist to strictly order renderStatsBlock w.r.t. updateGraphs
  renderAll: function (desc, configuration) {
    if (this.drawnDesc !== desc) {
      this.renderStatsBlock(desc);
      this.drawnDesc = desc;
    }
    if (this.drawnConfiguration !== configuration) {
      this.updateGraphs(configuration);
      this.drawnConfiguration = configuration;
    }
  },
  unrenderNothing: function () {
    if (this.spinners) {
      _.each(this.spinners, function (s) {s.remove()});
      this.spinners = null;
    }
  },
  renderNothing: function () {
    if (this.spinners) {
      return;
    }
    this.spinners = [
      overlayWithSpinner(this.largeGraphJQ)
    ];
    this.smallGraphsContainerJQ.find('.small_graph_label .value').text('?')
  },
  renderStatsBlock: function (descValue) {
    if (!descValue) {
      this.smallGraphsContainerJQ.html('');
      this.renderNothing();
      return;
    }
    this.unrenderNothing();
    renderTemplate('new_stats_block', descValue, this.smallGraphsContainerJQ[0]);
    $(this).trigger('menelaus.graphs-widget.rendered-stats-block');
  },
  zoomToSeconds: {
    minute: 60,
    hour: 3600,
    day: 86400,
    week: 691200,
    month: 2678400,
    year: 31622400
  },
  forceNextRendering: function () {
    this.lastCompletedTimestamp = undefined;
  },
  updateGraphs: function (configuration) {
    var self = this;

    if (!configuration) {
      self.lastCompletedTimestamp = undefined;
      return self.renderNothing();
    }

    self.unrenderNothing();

    var nowTStamp = (new Date()).valueOf();
    if (self.lastCompletedTimestamp && nowTStamp - self.lastCompletedTimestamp < 200) {
      // skip this sample as we're too slow
      return;
    }

    var stats = configuration.samples;

    var timeOffset = configuration.clientDate - configuration.serverDate;

    // TODO: empty stats should be maybe handled elsewhere
    if (!stats) {
      stats = {timestamp: []};
      _.each(_.keys(configuration.infos), function (name) {
        stats[name] = [];
      });
    }

    var zoomMillis = (self.zoomToSeconds[configuration.zoomLevel] || 60) * 1000;
    var selected = configuration.selected;
    var now = (new Date()).valueOf();
    if (configuration.interval < 2000) {
      now -= StatsModel.samplesBufferDepth.value * 1000;
    }

    maybeReloadAppDueToLeak();

    plotStatGraph(self.largeGraphJQ, stats[selected.name], configuration.timestamp, {
      color: '#1d88ad',
      verticalMargin: 1.02,
      fixedTimeWidth: zoomMillis,
      timeOffset: timeOffset,
      lastSampleTime: now,
      breakInterval: configuration.interval * 2.5,
      maxY: configuration.infos.byName[selected.name].maxY
    });

    try {
      var visibleBlockIDs = {};
      _.each($(_.map(configuration.infos.blockIDs, $i)).filter(":has(.stats:visible)"), function (e) {
        visibleBlockIDs[e.id] = e;
      });
    } catch (e) {
      debugger
      throw e;
    }

    _.each(configuration.infos, function (statInfo) {
      if (!visibleBlockIDs[statInfo.blockId]) {
        return;
      }
      var statName = statInfo.name;
      var graphContainer = $($i(statInfo.id));
      if (graphContainer.length == 0) {
        return;
      }
      renderSmallGraph(graphContainer, {
        data: stats[statName] || [],
        breakInterval: configuration.interval * 2.5,
        timeOffset: timeOffset,
        now: now,
        zoomMillis: zoomMillis,
        isSelected: selected.name == statName,
        timestamp: configuration.timestamp,
        maxY: configuration.infos.byName[statName].maxY
      });
    });

    self.lastCompletedTimestamp = (new Date()).valueOf();

    $(self).trigger('menelaus.graphs-widget.rendered-graphs');
  }
});

var AnalyticsSection = {
  onKeyStats: function (hotKeys) {
    $('#top_keys_block').need(1).toggle(hotKeys !== null);
    if (hotKeys == null) {
      return;
    }
    renderTemplate('top_keys', _.map(hotKeys, function (e) {
      return $.extend({}, e, {total: 0 + e.gets + e.misses});
    }));
    $('#top_keys_container table tr:has(td):odd').addClass('even');
  },
  init: function () {
    var self = this;

    StatsModel.hotKeysCell.subscribeValue($m(self, 'onKeyStats'));
    prepareTemplateForCell('top_keys', StatsModel.hotKeysCell);

    $('#analytics .block-expander').live('click', function () {
      // this forces configuration refresh and graphs redraw
      self.widget.forceNextRendering();
      StatsModel.configurationExtra.setValue({});
    });

    IOCenter.staleness.subscribeValue(function (stale) {
      $('.stats-period-container')[stale ? 'hide' : 'show']();
      $('#analytics .staleness-notice')[stale ? 'show' : 'hide']();
    });

    (function () {
      var cell = Cell.compute(function (v) {
        var mode = v.need(DAL.cells.mode);
        if (mode != 'analytics') {
          return;
        }

        var allBuckets = v.need(DAL.cells.bucketsListCell);
        var selectedBucket = v.need(StatsModel.statsBucketDetails);
        return {list: _.map(allBuckets, function (info) {return [info.uri, info.name]}),
                selected: selectedBucket.uri};
      });
      $('#analytics_buckets_select').bindListCell(cell, {
        onChange: function (e, newValue) {
          StatsModel.statsBucketURL.setValue(newValue);
        }
      });
    })();

    (function () {
      var cell = Cell.compute(function (v) {
        var mode = v.need(DAL.cells.mode);
        if (mode != 'analytics') {
          return;
        }

        var allNodes = v.need(StatsModel.statsNodesCell);
        var selectedNode;
        var statsHostname = v(StatsModel.statsHostname);

        if (statsHostname) {
          selectedNode = v.need(StatsModel.targetCell);
        }

        var allNames = _.pluck(allNodes.servers, 'hostname');

        var list = _.map(allNodes.servers, function (srv) {
          var name = ViewHelpers.maybeStripPort(srv.hostname, allNodes.servers);
          return [srv.hostname, name];
        });
        // natural sort by full hostname (which includes port number)
        list.sort(mkComparatorByProp(0, naturalSort));
        list.unshift(['', 'All Server Nodes']);

        return {list: list,
                selected: selectedNode && selectedNode.hostname};
      });
      $('#analytics_servers_select').bindListCell(cell, {
        onChange: function (e, newValue) {
          StatsModel.statsHostname.setValue(newValue || undefined);
        }
      });
    })();

    self.widget = new GraphsWidget($('#analytics_main_graph'), $('#stats_container'), StatsModel.statsDescInfoCell, StatsModel.graphsConfigurationCell);

    Cell.needing(StatsModel.graphsConfigurationCell).compute(function (v, configuration) {
      return configuration.timestamp.length == 0;
    }).subscribeValue(function (missingSamples) {
      $('.stats-period-container').toggleClass('missing-samples', !!missingSamples);
    });
    Cell.needing(StatsModel.graphsConfigurationCell).compute(function (v, configuration) {
      var zoomMillis = GraphsWidget.prototype.zoomToSeconds[configuration.zoomLevel] * 1000;
      return Math.ceil(Math.min(zoomMillis, configuration.serverDate - configuration.timestamp[0]) / 1000);
    }).subscribeValue(function (visibleSeconds) {
      $('.stats_visible_period').text(isNaN(visibleSeconds) ? '?' : formatUptime(visibleSeconds));
    });

    StatsModel.serverResourcesVisible.subscribeValue(function (visible) {
      $('#analytics')[visible ? 'addClass' : 'removeClass']('with_server_resources');
    });

    (function () {
      var selectionCell;
      StatsModel.smallGraphSelectionCellCell.subscribeValue(function (cell) {
        selectionCell = cell;
      });

      $(".analytics-small-graph").live("click", function (e) {
        // don't intercept right arrow clicks
        if ($(e.target).is(".right-arrow, .right-arrow *")) {
          return;
        }
        e.preventDefault();
        var graphParam = this.getAttribute('data-graph');
        if (!graphParam) {
          debugger
          throw new Error("shouldn't happen");
        }
        if (!selectionCell) {
          return;
        }
        selectionCell.setValue(graphParam);
        self.widget.forceNextRendering();
      });
    })();

    (function () {
      function handler() {
        var val = effectiveSelected.value;
        val = val && val.name;
        $('.analytics-small-graph.selected').removeClass('selected');
        if (!val) {
          return;
        }
        $(_.filter($('.analytics-small-graph[data-graph]'), function (el) {
          return String(el.getAttribute('data-graph')) === val;
        })).addClass('selected');
      }
      var effectiveSelected = Cell.compute(function (v) {return v.need(StatsModel.graphsConfigurationCell).selected});
      effectiveSelected.subscribeValue(handler);
      $(self.widget).bind('menelaus.graphs-widget.rendered-stats-block', handler);
    })();

    $(self.widget).bind('menelaus.graphs-widget.rendered-graphs', function () {
      var graph = StatsModel.graphsConfigurationCell.value.selected;
      $('#analytics .current-graph-name').text(graph.title);
      $('#analytics .current-graph-desc').text(graph.desc);
    });

    $(self.widget).bind('menelaus.graphs-widget.rendered-stats-block', function () {
      $('#stats_container').hide();
      _.each($('.analytics-small-graph:not(.dim)'), function (e) {
        e = $(e);
        var graphParam = e.attr('data-graph');
        if (!graphParam) {
          debugger
          throw new Error("shouldn't happen");
        }
        var ul = e.find('.right-arrow ul').need(1);

        var params = {sec: 'analytics', statsBucket: StatsModel.statsBucketDetails.value.uri};
        var aInner;

        if (!StatsModel.displayingSpecificStatsCell.value) {
          params.statsStatName = graphParam;
          aInner = "show by server";
        } else {
          params.graph = StatsModel.statsStatName.value;
          params.statsHostname = graphParam;
          aInner = "show this server";
        }
        var a = $('<a>' + aInner + '</a>')[0];
        a.setAttribute('href', '#' + $.param(params));
        var li = document.createElement('LI');
        li.appendChild(a);
        ul[0].appendChild(li);
      });
      $('#stats_container').show();
    });

    StatsModel.displayingSpecificStatsCell.subscribeValue(function (displayingSpecificStats) {
      displayingSpecificStats = !!displayingSpecificStats;
      $('#analytics .when-normal-stats').toggle(!displayingSpecificStats);
      $('#analytics .when-specific-stats').toggle(displayingSpecificStats);
    });

    Cell.subscribeMultipleValues(function (specificStatsNamesSet, statsStatName) {
      var text = '?';
      if (specificStatsNamesSet && statsStatName) {
        var pair = specificStatsNamesSet.byName[statsStatName];
        if (pair) {
          text = pair[1];
        } else {
          debugger
        }
      }
      $('.specific-stat-description').text(text);
    }, StatsModel.specificStatsNamesSetCell, StatsModel.effectiveSpecificStatName);
  },
  onLeave: function () {
    setHashFragmentParam("zoom", undefined);
    StatsModel.statsHostname.setValue(undefined);
    StatsModel.statsBucketURL.setValue(undefined);
    StatsModel.selectedGraphNameCell.setValue(undefined);
    StatsModel.statsStatName.setValue(undefined);
  },
  onEnter: function () {
  }
};
