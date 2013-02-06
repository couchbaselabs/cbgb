function ServerCtrl($scope, $http) {
  $http.get('/api/serverSettings').success(function(data) {
      $scope.serverSettings = data;
  });
  $http.get('/api/bucketSettings').success(function(data) {
      $scope.bucketSettings = data;
  });
}
