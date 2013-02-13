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

function BucketStatsCtrl($scope, $routeParams, $http, $timeout) {
  var bucketName = $routeParams.bucketName;

  $scope.changeLevel = function(i) {
    $scope.currLevel = i;
    if ($scope.timeout) {
      $timeout.cancel($scope.timeout);
      $scope.timeout = null;
    }
    if ($scope.drawChart) {
      $scope.drawChart(null);
      $scope.drawChart = null;
    }
    go();
  };

  function go() {
    $http.get('/api/buckets/' + bucketName + '/stats').
      success(function(data) {
        $scope.bucketName = bucketName;
        $scope.bucketStats = data;
        $scope.statNames = _.keys(data.totals.bucketStats);
        $scope.err = null;
        if (!$scope.drawChart) {
          $scope.drawChart = makeChart("ops", 60, 10, 300);
        }
        $scope.drawChart(data.diffs.bucketStats.levels[$scope.currLevel]);
        $scope.timeout = $timeout(go, 1000);
      }).
      error(function() {
        $scope.err = restErrorMsg;
        $scope.timeout = $timeout(go, 1000);
      });
  }

  $scope.changeLevel(0);
}

function makeChart(statName, dataLength, barW, barH) {
  var duration = 1000;
  var xMargin = 0.5;
  var yMargin = 0.5;
  var done = false;

  var xScale = d3.scale.linear()
    .domain([0, 1])
    .range([0, barW]);

  return function(data) {
    if (!data) {
      done = true;
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

    var chart = d3.select("#chart")
      .attr("width", barW * dataLength - 1)
      .attr("height", barH);

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
