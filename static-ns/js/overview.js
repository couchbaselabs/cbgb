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
var OverviewSection = {
  initLater: function () {
    var self = this;
    var overview = $('#overview');
    DAL.cells.bucketsListCell.subscribeValue(function (buckets) {
      overview.find('.buckets-number').text(buckets ? ViewHelpers.count(buckets.length, 'bucket') : '??');
      if (buckets && buckets.length === 0) {
        overview.find('.no_stats_marker').addClass('no_buckets');
      } else {
        overview.find('.no_stats_marker').removeClass('no_buckets');
      }
    });

    DAL.cells.serversCell.subscribeValue(function (servers) {
      var reallyActiveNodes = _.select(servers ? servers.active : [], function (n) {
        return n.clusterMembership == 'active';
      });
      $('#active-servers-count').text(servers ? reallyActiveNodes.length : '??');

      if (!servers) {
        self.serversBlockJQ.find('.badge').hide();
        return;
      }
      var pending = servers.pending;
      var failedOver = _.select(servers.allNodes, function (node) {
        return node.clusterMembership == 'inactiveFailed';
      });
      var down = _.select(servers.allNodes, function (node) {
        return node.status != 'healthy';
      });

      function updateCount(jq, count) {
        jq.text(count)
          .closest('.badge')[count ? 'show' : 'hide']()
          .closest('li')[count ? 'removeClass' : 'addClass']('is-zero');
      }

      updateCount(self.failedOverCountJQ, failedOver.length);
      updateCount(self.downCountJQ, down.length);
      updateCount(self.pendingCountJQ, pending.length);
    });

    IOCenter.staleness.subscribeValue(function (stale) {
      overview.find('.staleness-notice')[stale ? 'show' : 'hide']();
    });

    DAL.cells.currentPoolDetailsCell.subscribeValue(function (poolDetails) {
      if (!poolDetails || !poolDetails.storageTotals
          || !poolDetails.storageTotals.ram || !poolDetails.storageTotals.hdd) {
        self.clustersBlockJQ.hide();
        return;
      }
      self.clustersBlockJQ.show();

      ;(function () {
        var item = self.clustersBlockJQ.find('.ram-item');

        var ram = poolDetails.storageTotals.ram;
        var usedQuota = ram.usedByData;
        var bucketsQuota = ram.quotaUsed;
        var quotaTotal = ram.quotaTotal;
        var gaugeOptions = {
          topAttrs: {'class': 'line_cnt'},
          usageAttrs: {'class': 'usage_biggest'},
          topLeft: ['Total Allocated', ViewHelpers.formatMemSize(bucketsQuota)],
          topRight: ['Total in Cluster', ViewHelpers.formatMemSize(quotaTotal)],
          items: [
            {name: 'In Use',
             value: usedQuota,
             attrs: {style: 'background-color:#00BCE9'},
             tdAttrs: {style: 'color:#1878a2;'}},
            {name: 'Unused',
             value: bucketsQuota - usedQuota,
             attrs: {style: 'background-color:#7EDB49'},
             tdAttrs: {style: 'color:#409f05;'}},
            {name: 'Unallocated',
             value: Math.max(quotaTotal - bucketsQuota, 0),
             attrs: {style: 'background-color:#E1E2E3'},
             tdAttrs: {style: 'color:#444245;'}}
          ],
          markers: [{value: bucketsQuota,
                     attrs: {style: 'background-color:#e43a1b'}}]
        };
        if (gaugeOptions.items[1].value < 0) {
          gaugeOptions.items[0].value = bucketsQuota;
          gaugeOptions.items[1] = {
            name: 'Overused',
            value: usedQuota - bucketsQuota,
            attrs: {style: 'background-color: #F40015;'},
            tdAttrs: {style: 'color:#e43a1b;'}
          };
          if (usedQuota < quotaTotal) {
            _.extend(gaugeOptions.items[2], {
              name: 'Available',
              value: quotaTotal - usedQuota
            });
          } else {
            gaugeOptions.items.length = 2;
            gaugeOptions.markers << {value: quotaTotal,
                                     attrs: {style: 'color:#444245;'}}
          }
        }

        item.find('.line_cnt').replaceWith(memorySizesGaugeHTML(gaugeOptions));
      })();

      ;(function () {
        var item = self.clustersBlockJQ.find('.disk-item');

        var hdd = poolDetails.storageTotals.hdd;

        var usedSpace = hdd.usedByData;
        var total = hdd.total;
        var other = hdd.used - usedSpace;
        var free = hdd.free;

        item.find('.line_cnt').replaceWith(memorySizesGaugeHTML({
          topAttrs: {'class': 'line_cnt'},
          usageAttrs: {'class': 'usage_biggest'},
          topLeft: ['Usable Free Space', ViewHelpers.formatMemSize(free)],
          topRight: ['Total Cluster Storage', ViewHelpers.formatMemSize(total)],
          items: [
            {name: 'In Use',
             value: usedSpace,
             attrs: {style: 'background-color:#00BCE9'},
             tdAttrs: {style: "color:#1878A2;"}},
            {name: 'Other Data',
             value: other,
             attrs: {style:"background-color:#FDC90D"},
             tdAttrs: {style:"color:#C19710;"}},
            {name: "Free",
             value: total - other - usedSpace,
             attrs: {style: 'background-color:#E1E2E3'},
             tdAttrs: {style:"color:#444245;"}}
          ],
          markers: [
            {value: other + usedSpace + free,
             attrs: {style: "background-color:#E43A1B;"}}
          ]
        }));
      })();
    });
  },
  init: function () {
    _.defer($m(this, 'initLater'));

    this.opsGraphJQ = $('#overview_ops_graph');
    this.readsGraphJQ = $('#overview_reads_graph');
    this.graphAreaJQ = this.opsGraphJQ.closest('.item');
    this.clustersBlockJQ = $('#overview_clusters_block');
    this.serversBlockJQ = $('#overview_servers_block');
    this.failedOverCountJQ = this.serversBlockJQ.find('.failed-over-count');
    this.downCountJQ = this.serversBlockJQ.find('.down-count');
    this.pendingCountJQ = this.serversBlockJQ.find('.pending-count');

    // this fake cell makes sure our stats cell is not recomputed when pool details change
    this.statsCellArg = new Cell(function (poolDetails, mode) {
      if (mode != 'overview')
        return;
      return 1;
    }, {
      poolDetails: DAL.cells.currentPoolDetailsCell,
      mode: DAL.cells.mode
    });

    this.statsCell = Cell.needing(this.statsCellArg).compute(function (v, arg) {
      return future.get({url: '/pools/default/overviewStats'});
    });

    this.statsCell.keepValueDuringAsync = true;
    this.statsCell.subscribe(function (cell) {
      var ts = cell.value.timestamp;
      var interval = ts[ts.length-1] - ts[0];
      if (isNaN(interval) || interval < 15000)
        cell.recalculateAfterDelay(2000);
      else if (interval < 300000)
        cell.recalculateAfterDelay(10000);
      else
        cell.recalculateAfterDelay(30000);
    });

    this.statsToRenderCell = new Cell(function (stats, mode) {
      if (mode == 'overview')
        return stats;
    }, {
      mode: DAL.cells.mode,
      stats: this.statsCell
    })
    this.statsToRenderCell.subscribeValue($m(this, 'onStatsValue'));
  },
  plotGraph: function (graphJQ, stats, attr) {
    var now = (new Date()).valueOf();
    var tstamps = stats.timestamp || [];
    var breakInterval;
    if (tstamps.length > 1) {
      breakInterval = (tstamps[tstamps.length-1] - tstamps[0]) /
        Math.min(tstamps.length / 2, 30);
    }

    plotStatGraph(graphJQ, stats[attr], tstamps, {
      lastSampleTime: now,
      breakInterval: breakInterval,
      processPlotOptions: function (plotOptions, plotDatas) {
        var firstData = plotDatas[0];
        var t0 = firstData[0][0];
        var t1 = now;
        if (t1 - t0 < 300000) {
          plotOptions.xaxis.ticks = [t0, t1];
          plotOptions.xaxis.tickSize = [null, "minute"];
        }
        return plotOptions;
      }
    });
  },
  onStatsValue: function (stats) {
    var haveStats = true;
    if (!stats || DAL.cells.mode.value != 'overview')
      haveStats = false;
    else if (stats.timestamp.length < 2)
      haveStats = false;

    if (haveStats) {
      this.graphAreaJQ.removeClass('no-samples-yet');
      this.plotGraph(this.opsGraphJQ, stats, 'ops');
      this.plotGraph(this.readsGraphJQ, stats, 'ep_bg_fetched');
    } else {
      this.graphAreaJQ.addClass('no-samples-yet');
    }
  },
  onEnter: function () {
  }
};
