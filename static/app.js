angular.module('cbgb', []).
  config(['$routeProvider', function($routeProvider) {
  $routeProvider.
      when('/server',
           {templateUrl: 'partials/server.html',
            controller: ServerCtrl}).
      when('/buckets',
           {templateUrl: 'partials/bucket-list.html',
            controller: BucketListCtrl}).
      when('/buckets/:bucketName',
           {templateUrl: 'partials/bucket-detail.html',
            controller: BucketDetailCtrl}).
      when('/buckets/:bucketName/stats',
           {templateUrl: 'partials/bucket-stats.html',
            controller: BucketStatsCtrl}).
      otherwise({redirectTo: '/server'});
}]);

var restErrorMsg = "error communicating with server; please try again.";

function ServerCtrl($scope, $http) {
  $http.get('/api/settings').
    success(function(data) {
      $scope.settings = data;
      $scope.err = null;
    }).
    error(function() {
      $scope.err = restErrorMsg
    });
}

function BucketListCtrl($scope, $http) {
  $http.get('/api/buckets').
    success(function(data) {
      $scope.names = data;
      $scope.err = null;
    }).
    error(function() {
      $scope.err = restErrorMsg
    });
}

function BucketDetailCtrl($scope, $routeParams, $http) {
  $http.get('/api/buckets/' + $routeParams.bucketName).
    success(function(data) {
      $scope.bucket = data;
      $scope.bucket.partitionsArray = _.values(data.partitions);
      $scope.err = null;
    }).
    error(function() {
      $scope.err = restErrorMsg
    });

  $scope.orderChoice = 'id';
}

var lastChartId = (new Date()).getTime();

function BucketStatsCtrl($scope, $routeParams, $http, $timeout) {
  $scope.bucketName = $routeParams.bucketName;
  $scope.currLevel = 0;
  $scope.currStatKind = "bucketStats";
  $scope.currStatName = "ops";

  $scope.stopChart = function() {
    if ($scope.drawChart) {
      $scope.drawChart(null);
      $scope.drawChart = null;
    }
    if ($scope.timeout) {
      $timeout.cancel($scope.timeout);
      $scope.timeout = null;
    }
  }

  $scope.changeLevel = function(i) {
    $scope.stopChart();
    $scope.currLevel = i;
    go();
  }

  $scope.changeStat = function(kind, statName) {
    $scope.stopChart();
    $scope.currStatKind = kind;
    $scope.currStatName = statName;
    go();
  }

  function go() {
    $http.get('/api/buckets/' + $scope.bucketName + '/stats').
      success(function(data) {
        $scope.err = null;
        $scope.stats = data;
        $scope.statNames =
          _.union(_.map(_.without(_.keys(data.totals.bucketStats), "time"),
                        function(x) { return ["bucketStats", x]; }),
                  _.map(_.without(_.keys(data.totals.bucketStoreStats), "time"),
                        function(x) { return ["bucketStoreStats", x]; }))

        if (!$scope.drawChart) {
          lastChartId++;
          $scope.drawChart =
            makeChart(lastChartId,
                      $scope.currStatName,
                      data.levels[$scope.currLevel].numSamples,
                      10, 400);
        }
        $scope.drawChart(data.diffs[$scope.currStatKind].levels[$scope.currLevel]);
        $scope.timeout = $timeout(go, 1000);
      }).
      error(function() {
        $scope.err = restErrorMsg;
        $scope.timeout = $timeout(go, 1000);
      });
  }

  go();
}

function makeChart(chartId, statName, dataLength, barW, barH) {
  var duration = 400;
  var xMargin = 0.5;
  var yMargin = 0.5;
  var done = false;

  var xScale = d3.scale.linear()
    .domain([0, 1])
    .range([0, barW]);

  return function(data) {
    if (!data || lastChartId != chartId) {
      done = true;
      d3.select("#chart" + chartId)
        .selectAll("rect")
        .data([])
        .exit()
        .remove();
      d3.select("#chart" + chartId)
        .remove();
    }
    if (done) {
      return;
    }

    function idx(i) {
      // Handles when data length is too small.
      if (dataLength > data.length) {
        return i + dataLength - data.length;
      }
      return i;
    }

    var yMin = 0xffffffff;
    var yMax = 0;
    for (var i = 0; i < data.length; i++) {
      if (yMin > data[i][statName]) {
        yMin = data[i][statName];
      }
      if (yMax < data[i][statName]) {
        yMax = data[i][statName];
      }
    }
    if (yMax - yMin < barH) {
      yMax = yMin + barH;
    }

    var yScale = d3.scale.linear()
      .domain([yMin, yMax])
      .rangeRound([2, barH]);

    if (!document.getElementById("chart" + chartId)) {
      d3.select("#charts")
        .append("svg:svg")
        .attr("id", "chart" + chartId)
        .attr("class", "chart")
        .attr("width", barW * dataLength - 1)
        .attr("height", barH);
    }

    var chart = d3.select("#chart" + chartId);

    var rect = chart.selectAll("rect")
      .data(data, function(d) { return d.time; })

    rect.enter()
      .insert("rect")
      .attr("x", function(d, i) { return xScale(idx(i + 1)) - xMargin; })
      .attr("y", function(d) {
          return barH - yScale(d[statName]) - yMargin;
        })
      .attr("width", barW)
      .attr("height", function(d) { return yMin + yScale(d[statName]); })
      .transition().duration(duration)
      .attr("x", function(d, i) { return xScale(idx(i)) - xMargin; });

    rect.transition().duration(duration)
      .attr("x", function(d, i) { return xScale(idx(i)) - xMargin; })
      .attr("y", function(d) {
          return barH - yScale(d[statName]) - yMargin;
        })
      .attr("height", function(d) { return yMin + yScale(d[statName]); });

    rect.exit()
      .transition().duration(duration)
      .attr("x", function(d, i) { return -barW - xMargin; })
      .remove();
  }
}
