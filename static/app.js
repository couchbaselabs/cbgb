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
      otherwise({redirectTo: '/server'});
}]);

function ServerCtrl($scope, $http) {
  $http.get('/api/settings').success(function(data) {
      $scope.settings = data;
  });
}

function BucketListCtrl($scope, $http) {
  $http.get('/api/buckets').success(function(data) {
      $scope.names = data;
  });
}

function BucketDetailCtrl($scope, $routeParams, $http) {
  $http.get('/api/buckets/' + $routeParams.bucketName).success(function(data) {
      $scope.bucket = data;
      $scope.bucket.partitionsArray = _.values(data.partitions);
  });
  $scope.orderChoice = 'id';
}
