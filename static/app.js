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

function BucketStatsCtrl($scope, $routeParams, $http) {
  var bucketName = $routeParams.bucketName
  $http.get('/api/buckets/' + bucketName + '/stats').
    success(function(data) {
      $scope.bucketName = bucketName;
      $scope.bucketStats = data;
      $scope.err = null;
      go();
    }).
    error(function() {
      $scope.err = restErrorMsg
    });
}

function go() {
 var r = 200;
 var vis = d3.select("#chart");

 var t = 1297110663, // start time (seconds since epoch)
     v = 70, // start value (subscribers)
     data = d3.range(33).map(next); // starting dataset

 function next() {
  return {
    time: ++t,
    value: v = ~~Math.max(10, Math.min(90, v + 10 * (Math.random() - .5)))
  };
 }

 interval = setInterval(function() {
    data.shift();
    data.push(next());
    redraw();
 }, 1500);

 var w = 10,
     h = 80;

 var x = d3.scale.linear()
    .domain([0, 1])
    .range([0, w]);

 var y = d3.scale.linear()
    .domain([0, 100])
    .rangeRound([0, h]);

 var chart = d3.select("#chart")
    .attr("class", "chart")
    .attr("width", w * data.length - 1)
    .attr("height", h);

 chart.selectAll("rect")
    .data(data)
  .enter().append("rect")
    .attr("x", function(d, i) { return x(i) - .5; })
    .attr("y", function(d) { return h - y(d.value) - .5; })
    .attr("width", w)
    .attr("height", function(d) { return y(d.value); });

 function redraw() {
  var rect = chart.selectAll("rect")
      .data(data, function(d) { return d.time; });

  rect.enter().insert("rect", "line")
      .attr("x", function(d, i) { return x(i + 1) - .5; })
      .attr("y", function(d) { return h - y(d.value) - .5; })
      .attr("width", w)
      .attr("height", function(d) { return y(d.value); })
    .transition()
      .duration(1000)
      .attr("x", function(d, i) { return x(i) - .5; });

  rect.transition()
      .duration(1000)
      .attr("x", function(d, i) { return x(i) - .5; });

  rect.exit().transition()
      .duration(1000)
      .attr("x", function(d, i) { return x(i - 1) - .5; })
      .remove();
 }
}
