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

 var documentErrorDef = {
  '502': 'The node containing that document is currently down',
  '503': 'The data has not yet loaded, please wait...',
  '404': 'A document with that ID does not exist',
  'unknown': 'There was an unexpected error'
};

// Builds URL for getting/updating given document via CouchDB API
// NOTE: couch does redirect when doing _design%2FXX access, so we
// need special method, instead of using generic buildURL
function buildDocURL(base, docId/*, ..args */) {
  // return Cell with to-be-built URL when any of arguments is a cell
  var args = _.toArray(arguments);
  if ((base instanceof Cell) || (docId instanceof Cell)) {
    return Cell.applyFunctionWithResolvedValues(buildDocURL, this, args);
  }

  if (base.charAt(base.length-1) !== '/') {
    args[0] += '/'; // NOTE: this is base but it's used as part of
                    // args
  }

  if (docId.slice(0, "_design/".length) === "_design/") {
    args.splice(1, 1, "_design", docId.slice("_design/".length));
  } else if (docId.slice(0, "_local/".length) === "_local/") {
    args.splice(1, 1, "_local", docId.slice("_local/".length));
  }
  return buildURL.apply(null, args);
}

function buildViewPseudoLink(bucketName, ddoc, type, viewName) {
  return encodeURIComponent(_.map([bucketName, ddoc.meta.id, type, viewName], encodeURIComponent).join("/"));
}

function unbuildViewPseudoLink(link, body, context) {
  if (link.indexOf("/") < 0) {
    link = decodeURIComponent(link);
  }
  body = body || _.toArray;
  return body.apply(context, _.map(link.split("/"), decodeURIComponent));
}

function couchGet(url, callback) {
  IOCenter.performGet({url: url, dataType: "json",
    success: function(body, status, xhr) {
      var meta = xhr.getResponseHeader("X-Couchbase-Meta");
      if (meta) { // we have a doc not a view
        body = {
          json : body,
          meta : JSON.parse(meta)
        }
      }
      callback.call(this, body, status, xhr);
    }
  });
}

// now we can pass Cell instead of url and get callback called when
// url cell has value and that url is fetched. Note that passing cells
// that may not ever get defined value is _dangerous_ and will leak.
couchGet = Cell.wrapWithArgsResolving(couchGet);
// same for couchReq, below
couchReq = Cell.wrapWithArgsResolving(couchReq);

function couchReq(method, url, data, success, error, extraArgs) {
  var postData = _.extend({}, extraArgs || {}, {
    type: method,
    url: url,
    dataType: 'json',
    contentType: 'application/json',
    success: function(doc, status, xhr) {
      var meta = xhr.getResponseHeader("X-Couchbase-Meta");
      if (meta) {
        doc = {
          json : doc,
          meta : JSON.parse(meta)
        }
      }
      success.call(this, doc, status, xhr);
    },
    error: function (xhr) {
      var self = this;
      var args = arguments;
      var status = xhr.status;

      function handleUnexpected() {
        return onUnexpectedXHRError.apply(self, args);
      }

      if (!error) {
        return handleUnexpected();
      }

      var parsedJSON = {};

      try {
        parsedJSON = JSON.parse(xhr.responseText);
      } catch (e) {
        // ignore
      }

      return error(parsedJSON, status, handleUnexpected);
    }
  });

  if (data) {
    postData.data = JSON.stringify(data);
  }

  $.ajax(postData);
}

future.capiViewGet = function (ajaxOptions, valueTransformer, newValue, futureWrapper) {
  function handleError(xhr, xhrStatus, errMsg) {
    var error = {
      from: ajaxOptions.url
    }

    if (xhr.responseText) {
      response = JSON.parse(xhr.responseText);
      error.reason = response.error;
      error.explain = response.reason;
    }

    error.reason = error.reason || "unexpected";

    dataCallback({
      rows: [],
      errors: [error]
    });
  }

  var dataCallback;
  if (ajaxOptions.error || ajaxOptions.missingValueProducer || ajaxOptions.missingValue) {
    BUG();
  }
  ajaxOptions = _.clone(ajaxOptions);
  ajaxOptions.error = handleError;
  ajaxOptions.missingValueProducer = handleError;
  // we're using future wrapper to get reference to dataCallback
  // so that we can call it from error handler
  return future.get(ajaxOptions, valueTransformer, newValue, function (initXHR) {
    return (future || futureWrapper)(function (__dataCallback) {
      dataCallback = __dataCallback;
      initXHR(dataCallback);
    })
  });
}

function isDevModeDoc(ddoc) {
  var devPrefix = "_design/dev_";
  return ddoc.meta.id.substring(0, devPrefix.length) == devPrefix;
}

var codeMirrorOpts = {
  lineNumbers: true,
    matchBrackets: true,
    mode: "javascript",
    theme: 'default',
    tabSize: 2
};

// ns is assumed to have:
//   * rawViewsBucketCell
//   * rawDDocIdCell
//   * rawSpatialNameCell
//   * rawViewNameCell
//   * pageNumberCell
function createViewsCells(ns, bucketsListCell, capiBaseCell, modeCell, tasksProgressCell, poolDetailsCell) {

  ns.viewsBucketInfoCell = Cell.compute(function (v) {
    var selected = v(ns.rawViewsBucketCell);
    var buckets = v.need(bucketsListCell).byType.membase;
    var bucketInfo = _.detect(buckets, function (info) {return info.name === selected});
    if (bucketInfo) {
      return bucketInfo;
    }
    var bucketInfo = _.detect(buckets, function (info) {return info.name === "default"}) || buckets[0];

    return bucketInfo;
  }).name("viewsBucketInfoCell");

  ns.viewsBucketInfoCell.equality = function (a, b) {return a === b;};

  ns.viewsBucketCell = Cell.computeEager(function (v) {
    var bucketInfo = v(ns.viewsBucketInfoCell);
    if (!bucketInfo) {
      return null;
    }
    return bucketInfo.name;
  }).name("viewsBucketCell");
  ns.viewsBucketCell.equality = function (a, b) {return a === b;};

  ns.populateBucketsDropboxCell = Cell.compute(function (v) {
    var mode = v.need(modeCell);
    if (mode != 'views') {
      return;
    }

    var buckets = v.need(bucketsListCell).byType.membase;
    var selectedBucketName = v.need(ns.viewsBucketCell);
    return {list: _.map(buckets, function (info) {return [info.name, info.name]}),
            selected: selectedBucketName};
  });
  ns.haveBucketsCell = Cell.compute(function (v) {
    return v.need(ns.viewsBucketCell) !== null;
  });

  ns.selectedBucketCell = Cell.compute(function (v) {
    if (v.need(modeCell) != 'views')
      return;
    return v.need(ns.viewsBucketCell);
  }).name("selectedBucket");

  ns.dbURLCell = Cell.computeEager(function (v) {
    var base = v.need(capiBaseCell);
    var bucketName = v.need(ns.selectedBucketCell);

    if (bucketName) {
      return buildURL(base, bucketName) + "/";
    } else {
      return;
    }
  }).name("dbURLCell");

  ns.allDDocsURLCell = Cell.computeEager(function (v) {
    var bucketInfo = v.need(ns.viewsBucketInfoCell);
    return bucketInfo.ddocs.uri;
  }).name("allDDocsURL");

  var missingDDocsMarker = [];

  function missingDDocsValueProducer(xhr, options) {
    var responseJSON;
    try {
      responseJSON = $.parseJSON(xhr.responseText);
    } catch (e) {
      return undefined;
    }
    if (responseJSON.error === "no_ddocs_service") {
      return missingDDocsMarker;
    }
    return undefined;
  }

  ns.rawAllDDocsCell = Cell.compute(function (v) {
    return future.get({url: v.need(ns.allDDocsURLCell),
                       missingValueProducer: missingDDocsValueProducer});
  }).name("rawAllDDocs");

  ns.ddocsAreInFactMissingCell = Cell.compute(function (v) {
    return v.need(ns.rawAllDDocsCell) === missingDDocsMarker;
  }).name("ddocsAreInFactMissingCell");

  ns.allDDocsCell = Cell.compute(function (v) {
    var haveBuckets = v.need(ns.haveBucketsCell);

    if (haveBuckets) {
      return _.map(v.need(ns.rawAllDDocsCell).rows, function (r) {
        var doc = _.clone(r.doc);
        doc.compactURI = r.controllers.compact;
        doc.isDevModeDoc = isDevModeDoc(doc);
        return doc;
      });
    } else {
      return [];
    }
  }).name("allDDocs");
  ns.allDDocsCell.delegateInvalidationMethods(ns.rawAllDDocsCell);

  ns.currentDDocAndView = Cell.computeEager(function (v) {
    var ddocId = v(ns.rawDDocIdCell);
    var viewName = v(ns.rawViewNameCell);
    if (!ddocId || !viewName) {
      return [];
    }
    var allDDocs = v.need(ns.allDDocsCell);
    var ddoc = _.detect(allDDocs, function (d) {return d.meta.id === ddocId});
    if (!ddoc) {
      return [];
    }
    var view = (ddoc.json.views || {})[viewName];
    if (!view) {
      return [];
    }
    return [ddoc, viewName];
  }).name("currentDDocAndView");

  ns.currentDDocAndSpatial = Cell.computeEager(function (v) {
    var ddocId = v(ns.rawDDocIdCell);
    var spatialName = v(ns.rawSpatialNameCell);
    if (!ddocId || !spatialName) {
      return [];
    }
    var allDDocs = v.need(ns.allDDocsCell);
    var ddoc = _.detect(allDDocs, function (d) {return d.meta.id === ddocId});
    if (!ddoc) {
      return [];
    }
    var spatial = (ddoc.json.spatial || {})[spatialName];
    if (!spatial) {
      return [];
    }
    return [ddoc, spatialName];
  }).name("currentDDocAndSpatial");

  ns.populateViewsDropboxCell = Cell.compute(function (v) {
    var mode = v.need(modeCell);
    if (mode != 'views') {
      return;
    }

    var bucketName = v.need(ns.viewsBucketCell);

    var ddocs = _.sortBy(v.need(ns.allDDocsCell), function(ddoc) {
      return !ddoc.isDevModeDoc;
    });
    var ddocAndView = v.need(ns.currentDDocAndView);
    var ddocAndSpatial = v.need(ns.currentDDocAndSpatial);

    var selectedDDocId = ddocAndView.length && ddocAndView[0].meta.id ||
      ddocAndSpatial.length && ddocAndSpatial[0].meta.id;
    var selectedViewName = ddocAndView.length && ddocAndView[1] ||
      ddocAndSpatial.length && ddocAndSpatial[1];
    //var selectedType = ddocAndView.length > 0 ? '_view' : '_spatial';

    var devOptgroupOutput = false;
    var productionOptgroupOutput = false;
    var groups = _.map(ddocs, function (doc) {
      var rv = "";
      var viewNames = _.keys(doc.json.views || {}).sort();
      var spatialNames = _.keys(doc.json.spatial || {}).sort();

      if (doc.isDevModeDoc && !devOptgroupOutput) {
        rv += '<optgroup label="Development Views" class="topgroup">';
        devOptgroupOutput = true;
      } else if (!doc.isDevModeDoc && !productionOptgroupOutput) {
        rv += '</optgroup><optgroup label="Production Views" class="topgroup">';
        productionOptgroupOutput = true;
      }
      rv += '<optgroup label="' + escapeHTML(doc.meta.id) + '" class="childgroup">';
      _.each(viewNames, function (name) {
        var maybeSelected = (selectedDDocId === doc.meta.id &&
          selectedViewName === name) ? ' selected' : '';
        rv += '<option value="' +
          escapeHTML(buildViewPseudoLink(bucketName, doc, '_view', name)) +
          '"' + maybeSelected + ">" +
          escapeHTML(name) + "</option>";
      });
      _.each(spatialNames, function (name) {
        var maybeSelected = (selectedDDocId === doc.meta.id &&
          selectedViewName === name) ? ' selected' : '';
        rv += '<option value="' +
          escapeHTML(
            buildViewPseudoLink(bucketName, doc, '_spatial', name)) +
          '"' + maybeSelected + '>' +
          '[Spatial] ' + escapeHTML(name) + "</option>";
      });
      rv += "</optgroup>";
      return rv;
    });
    groups.push('</optgroup>');
    return {
      list: groups,
      selected: ddocAndView
    };
  });

  ns.currentView = Cell.compute(function (v) {
    return (function (ddoc, viewName) {
      if (viewName === undefined) {
        return;
      }
      return (ddoc.json.views || {})[viewName];
    }).apply(this, v.need(ns.currentDDocAndView));
  }).name("currentView");

  ns.currentSpatial = Cell.compute(function (v) {
    return (function (ddoc, spatialName) {
      if (spatialName === undefined) {
        return;
      }
      return (ddoc.json.spatial || {})[spatialName];
    }).apply(this, v.need(ns.currentDDocAndSpatial));
  }).name("currentSpatial");

  ns.editingDevView = Cell.compute(function (v) {
    var ddoc = v.need(ns.currentDDocAndView)[0];
    if (!ddoc) {
      return false;
    }
    return !!ddoc.meta.id.match(/^_design\/dev_/);
  }).name("editingDevView");

  ns.defaultSubsetViewResultsCell = Cell.compute(function (v) {
    return future.capiViewGet({url: v.need(ns.defaultSubsetResultsURLCell),
                               timeout: 3600000});
  }).name("defaultSubsetViewResultsCell")

  ns.fullSubsetViewResultsCell = Cell.compute(function (v) {
    return future.capiViewGet({url: v.need(ns.fullSubsetResultsURLCell),
                               timeout: 3600000});
  }).name("fullSubsetViewResultsCell")

  ns.viewResultsCell = Cell.compute(function (v) {
    // NOTE: we're requiring both subsets values, because otherwise
    // cells would deactivate 'other' results cell as not needed
    // anymore.
    var fullResults = v(ns.fullSubsetViewResultsCell);
    var defaultResults = v(ns.defaultSubsetViewResultsCell);
    return v.need(ns.subsetTabCell) === 'prod' ? fullResults : defaultResults;
  }).name("viewResultsCell");

  ns.viewResultsCellViews = Cell.compute(function(v) {
    v.need(ns.currentView);
    return v(ns.viewResultsCell);
  });

  ns.viewResultsCellSpatial = Cell.compute(function(v) {
    v.need(ns.currentSpatial);
    return v(ns.viewResultsCell);
  });

  ns.editingDevViewSpatial = Cell.compute(function (v) {
    var ddoc = v.need(ns.currentDDocAndSpatial)[0];
    if (!ddoc) {
      return false;
    }
    return !!ddoc.meta.id.match(/^_design\/dev_/);
  }).name("editingDevViewSpatial");

  ns.proposedURLBuilderCell = Cell.computeEager(function (v) {
    if (v(ns.currentView)) {
      var dbURL = v.need(ns.dbURLCell);
      var ddocAndView = v.need(ns.currentDDocAndView);
      if (!ddocAndView[1]) {
        return;
      }
      var filterParams = v.need(ViewsFilter.filterParamsCell);
      return function (pageNo, subset) {
        var ddoc = ddocAndView[0];
        var initial = {};
        if (subset === "prod") {
          initial.full_set = 'true';
        } else if (isDevModeDoc(ddoc)) {
          initial.stale = 'false'
        }
        return buildDocURL(dbURL, ddoc.meta.id, "_view", ddocAndView[1], _.extend(initial, filterParams, {
          limit: ViewsSection.PAGE_LIMIT.toString(),
          skip: String((pageNo - 1) * 10)
        }));
      };
    } else if (v(ns.currentSpatial)) {
      var dbURL = v.need(ns.dbURLCell);
      var ddocAndSpatial = v.need(ns.currentDDocAndSpatial);
      if (!ddocAndSpatial[1]) {
        return;
      }
      var filterParams = v.need(SpatialFilter.filterParamsCell);
      return function (pageNo, subset) {
        var initial = {};
        if (subset === "prod") {
          initial.full_set = 'true';
        }
        return buildDocURL(dbURL, ddocAndSpatial[0].meta.id, "_spatial", ddocAndSpatial[1], _.extend(initial, filterParams, {
          //limit: ViewsSection.PAGE_LIMIT.toString(),
          //skip: String((pageNo - 1) * 10)
          //bbox: "-180,-90,180,90"
        }));
      };
    }
  }).name("proposedURLBuilderCell");

  ns.intPageCell = Cell.computeEager(function (v) {
    var pageVal = v(ns.pageNumberCell);
    var pageNo = parseInt(pageVal, 10);
    if (isNaN(pageNo) || pageNo < 1) {
      pageNo = 1;
    }
    if (pageNo > 10) {
      pageNo = 10;
    }
    return pageNo;
  });

  ns.intFullSubsetPageCell = Cell.computeEager(function (v) {
    var pageVal = v(ns.fullSubsetPageNumberCell);
    var pageNo = parseInt(pageVal, 10);
    if (isNaN(pageNo) || pageNo < 1) {
      pageNo = 1;
    }
    if (pageNo > 10) {
      pageNo = 10;
    }
    return pageNo;
  });

  ns.defaultSubsetResultsURLCell = Cell.compute(function (v) {
    var appliedBuilder = v(ns.defaultSubsetAppliedURLBuilderCell);
    var proposedBuilder = v.need(ns.proposedURLBuilderCell);
    if (appliedBuilder !== proposedBuilder) {
      return;
    }
    return appliedBuilder(v.need(ns.intPageCell), "dev");
  });
  ns.fullSubsetResultsURLCell = Cell.compute(function (v) {
    var appliedBuilder = v(ns.fullSubsetAppliedURLBuilderCell);
    var proposedBuilder = v.need(ns.proposedURLBuilderCell);
    if (appliedBuilder !== proposedBuilder) {
      return;
    }
    return appliedBuilder(v.need(ns.intFullSubsetPageCell), "prod");
  });
  ns.viewResultsURLCell = Cell.compute(function (v) {
    if (v.need(ns.subsetTabCell) === "prod") {
      return v.need(ns.fullSubsetResultsURLCell);
    }
    return v.need(ns.defaultSubsetResultsURLCell);
  });
  ns.showHidePrevNextCell = Cell.compute(function (v) {
    var intCell = v.need(ns.subsetTabCell) === 'prod' ? ns.intFullSubsetPageCell : ns.intPageCell;
    return [v(ns.viewResultsCell), v.need(intCell)];
  });
  ns.productionDDocsCell = Cell.compute(function (v) {
    if (v.need(modeCell) != 'views') {
      return;
    }
    var allDDocs = v.need(ns.allDDocsCell);
    return _.select(allDDocs, function (ddoc) {
      return !ddoc.isDevModeDoc;
    });
  });

  ns.devDDocsCell = Cell.compute(function (v) {
    if (v.need(modeCell) != 'views') {
      return;
    }
    var allDDocs = v.need(ns.allDDocsCell);
    return _.select(allDDocs, function (ddoc) {
      return ddoc.isDevModeDoc
    });
  });

  // filters tasks of this bucket and converts them from array of
  // tasks to hash from ddoc id to array of tasks of on this ddoc
  // id.
  //
  // This also filters out non-view related tasks
  ns.tasksOfCurrentBucket = Cell.compute(function (v) {
    var tasks = v.need(tasksProgressCell);
    var bucketName = v.need(ns.selectedBucketCell);
    var rv = {};
    _.each(tasks, function (taskInfo) {
      if (taskInfo.type !== 'indexer' && taskInfo.type !== 'view_compaction') {
        return;
      }
      if (taskInfo.bucket !== bucketName) {
        return;
      }
      var ddoc = taskInfo.designDocument;
      var ddocTasks = rv[ddoc] || (rv[ddoc] = []);
      ddocTasks.push(taskInfo);
    });

    var importance = {
      view_compaction: 2,
      indexer: 1
    };

    _.each(rv, function (ddocTasks, key) {
      // lets also sort tasks in descending importance.
      // NOTE: js's sort is in-place
      ddocTasks.sort(function (taskA, taskB) {
        var importanceA = importance[taskA.type] || BUG("bad type");
        var importanceB = importance[taskB.type] || BUG("bad type");
        return importanceA - importanceB;
      });
    });

    return rv;
  }).name("tasksOfCurrentBucket");
  ns.tasksOfCurrentBucket.equality = _.isEqual;

  ns.rawForDoSaveAsView = Cell.computeEager(function (v) {
    var dbURL = v.need(ns.dbURLCell);
    var pair = v.need(ns.currentDDocAndView);
    return [dbURL].concat(pair);
  });

  ns.rawForDoSaveView = Cell.computeEager(function (v) {
    var dbURL = v.need(ns.dbURLCell);
    var currentView = v.need(ns.currentView);
    var pair = v.need(ns.currentDDocAndView);
    return [dbURL, currentView].concat(pair);
  });

  ns.rawForSpatialDoSaveAs = Cell.computeEager(function (v) {
    var dbURL = v.need(ns.dbURLCell);
    var pair = v.need(ns.currentDDocAndSpatial);
    return [dbURL].concat(pair);
  });

  ns.rawForSpatialDoSave = Cell.computeEager(function (v) {
    var dbURL = v.need(ns.dbURLCell);
    var currentSpatial = v.need(ns.currentSpatial);
    var pair = v.need(ns.currentDDocAndSpatial);
    return [dbURL, currentSpatial].concat(pair);
  });

  function mkViewsListCell(ddocsCell) {
    return Cell.compute(function (v) {
      var tasks = v.need(ns.tasksOfCurrentBucket);
      var ddocs = v.need(ddocsCell);
      var poolDetails = v.need(poolDetailsCell);

      var bucketName = v.need(ns.selectedBucketCell);
      var rv = _.map(ddocs, function (doc) {
        // that does deep cloning
        var rv = JSON.parse(JSON.stringify(doc));
        var viewInfos = _.map(rv.json.views || {}, function (value, key) {
          var plink = buildViewPseudoLink(bucketName, doc, '_view', key);
          return _.extend({name: key,
                           viewLink: '#showView=' + plink,
                           removeLink: '#removeView=' + plink
                          }, value);
        });
        viewInfos = _.sortBy(viewInfos, function (info) {return info.name;});
        rv.json.viewInfos = viewInfos;
        var spatialInfos = _.map(rv.json.spatial || {}, function (value, key) {
          var plink = buildViewPseudoLink(bucketName, doc, '_spatial', key);
          return _.extend({name: key,
                           viewLink: '#showSpatial=' + plink,
                           removeLink: '#removeSpatial=' + plink
                          }, value);
        });
        spatialInfos = _.sortBy(spatialInfos, function (info) {return info.name;});
        rv.json.spatialInfos = spatialInfos;
        return rv;
      });
      rv.ddocsAreInFactMissing = v(ns.ddocsAreInFactMissingCell);
      rv.tasks = tasks;
      rv.poolDetails = poolDetails;
      rv.bucketName = bucketName;
      return rv;
    });
  }

  ns.massagedDevDDocsCell = mkViewsListCell(ns.devDDocsCell);
  ns.massagedProductionDDocsCell = mkViewsListCell(ns.productionDDocsCell);
}



function createRandomDocCells(ns, modeCell) {
  function fetchRandomId(randomKeyURL, dbURL, dataCallback) {
    couchReq('GET', randomKeyURL, null,
             randomKeySuccess, randomKeyError);
    return;

    function randomKeySuccess(data) {
      dataCallback(data.key);
    };

    function randomKeyError(response, status, unexpected) {
      if (status == 404) {
        if (response.error == "fallback_to_all_docs") {
          console.log("using fallback to all docs");
          randomIdFromAllDocs();
          return;
        }
      }

      // TODO: we don't handle warmup very well and other 'expected'
      // errors like some nodes being down. So for now let's just fail
      // silently which is better than nothing
      allDocsError();
    };

    function randomIdFromAllDocs() {
      var allDocsURL = buildURL(dbURL, "_all_docs", {
        // precaution not to try to grab really huge number of docs because
        // of some error
        limit: 4096
      });

      couchReq('GET', allDocsURL, null, allDocsSuccess, allDocsError);
    }

    function allDocsSuccess(data) {
      var length = data.rows.length;

      if (length != 0) {
        var i = (Math.random() * data.rows.length) >> 0;
        var result = data.rows[i].id;
        dataCallback(result);
      } else {
        allDocsError();
      }
    }

    function allDocsError() {
      dataCallback(null);
    }
  }

  ns.randomKeyURLCell = Cell.compute(function (v) {
    var bucketInfo = v.need(ns.viewsBucketInfoCell);
    return bucketInfo.localRandomKeyUri;
  }).name("randomKeyURLCell");

  // null value of this cells means no docs exist
  ns.randomDocIdCell = Cell.computeEager(function (v) {
    if (v.need(modeCell) != 'views') {
      return;
    }
    var randomIdURL = v.need(ns.viewsBucketInfoCell).localRandomKeyUri;
    var dbURL = v.need(ns.dbURLCell);
    return future(function (dataCallback) {
      fetchRandomId(randomIdURL, dbURL, dataCallback);
    });
  });
  ns.randomDocIdCell.equality = function (a, b) {return a === b};

  // null value means no docs exist
  ns.sampleDocCell = Cell.computeEager(function (v) {
    // this will prevent loading random doc when it's not needed
    if (!v(ns.currentView) && !v(ns.currentSpatial)) {
      return;
    }
    var randomId = v(ns.sampleDocumentIdCell) || v(ns.randomDocIdCell);
    if (randomId == null) {//null or undefined
      return randomId;
    }

    var futureWrap = future.wrap(function (dataCallback, initateXHR) {
      function myCallback(body, status, xhr) {
        if (!body) {
          return dataCallback(body);
        }
        var meta = xhr.getResponseHeader("X-Couchbase-Meta");
        var trueBody = {
          json: body,
          meta: JSON.parse(meta)
        }
        dataCallback(trueBody)
      }
      initateXHR(myCallback);
    });
    var url = buildURL(v.need(ns.dbURLCell), randomId);
    return future.get({url: url,
                       missingValue: null
                      }, undefined, undefined, futureWrap)
  });
  ns.sampleDocCell.equality = function (a, b) {return a === b};
  ns.sampleDocCell.delegateInvalidationMethods(ns.randomDocIdCell);

  ns.viewResultsMassagedForTemplateCell = Cell.compute(function (v) {
    var viewResult = v(ns.viewResultsCellViews);
    if (!viewResult) {
      return {rows: {lackOfValue: true},
              errors: false}
    }

    var selectedBucket = v.need(ns.viewsBucketCell);

    var rows = _.filter(viewResult.rows, function (r) {return ('key' in r)});
    var errors = _.clone(viewResult.errors);
    if (errors) {
      // NOTE: we need snapshot of current tasks. I.e. we don't want
      // this cell to be recomputed when tasks change.
      var tasks = ns.tasksOfCurrentBucket.value;
      if (!tasks) {
        v(ns.tasksOfCurrentBucket);
        return;
      }
      var currentDoc = v.need(ns.rawDDocIdCell);

      var indexingRunning = _.filter(tasks[currentDoc], function (item) {
        return item.type == "indexer" && item.status == "running" && item.designDocument == currentDoc;
      }).length;

      _.each(errors, function (item, i) {
        if (item.reason == 'timeout' && indexingRunning) {
          item = errors[i] = _.clone(item);
          item.explain = "node is still building up the index";
          item.showBtn = true;
        }
      });
    }

    return {rows: rows, errors: errors, selectedBucket: selectedBucket};
  });
}

var ViewsSection = {
  timer: null,
  PAGE_LIMIT: 10,
  ensureBucketSelected: function (bucketName, body) {
    var self = this;
    ThePage.ensureSection("views");
    Cell.waitQuiescence(function () {
      self.viewsBucketCell.getValue(function (actualBucketName) {
        if (actualBucketName === bucketName) {
          return body();
        }
        self.rawViewsBucketCell.setValue(bucketName);
        ensureBucket(bucketName, body);
      })
    });
  },
  init: function () {
    var self = this;
    var views = $('#views');

    // you can get to local cells through this for debugging
    self.initEval = function (arg) {
      return eval(arg);
    }

    self.modeTabs = new TabsCell("vtab",
                                 "#views .tabs.switcher",
                                 "#views .panes > div",
                                 ["development", "production"]);
    self.modeTabs.subscribeValue(function (tab) {
      views[tab == 'development' ? 'addClass' : 'removeClass']('in-development');
      views[tab == 'development' ? 'removeClass' : 'addClass']('in-production');
    });

    self.subsetTabCell = new LinkClassSwitchCell('dev_subset', {
      firstItemIsDefault: true
    });
    self.subsetTabCell.addItem("subset_dev", "dev");
    self.subsetTabCell.addItem("subset_prod", "prod");
    self.subsetTabCell.finalizeBuilding();

    self.rawViewsBucketCell = new StringHashFragmentCell("viewsBucket");
    self.rawDDocIdCell = new StringHashFragmentCell("viewsDDocId");
    self.rawViewNameCell = new StringHashFragmentCell("viewsViewName");
    self.rawSpatialNameCell = new StringHashFragmentCell("viewsSpatialName");
    self.pageNumberCell = new StringHashFragmentCell("viewPage");
    self.fullSubsetPageNumberCell = new StringHashFragmentCell("fullSubsetViewPage");

    self.defaultSubsetAppliedURLBuilderCell = new Cell();
    self.fullSubsetAppliedURLBuilderCell = new Cell();
    self.lastAppliedURLBuilderCell = new Cell();
    self.sampleDocumentIdCell = new Cell();

    createViewsCells(self, DAL.cells.bucketsListCell, DAL.cells.capiBase, DAL.cells.mode, DAL.cells.tasksProgressCell, DAL.cells.currentPoolDetailsCell);
    createRandomDocCells(self, DAL.cells.mode);

    var viewcodeReduce = $('#viewcode_reduce');
    var viewcodeMap = $('#viewcode_map');
    var spatialcodeFun = $('#spatialcode_fun');

    self.mapEditor = CodeMirror.fromTextArea(viewcodeMap[0], codeMirrorOpts);
    self.reduceEditor = CodeMirror.fromTextArea(viewcodeReduce[0], codeMirrorOpts);
    self.spatialEditor = CodeMirror.fromTextArea(spatialcodeFun[0], codeMirrorOpts);

    var btnCreate = $('.btn_create', views);
    var viewsBucketSelect = $('#views_bucket_select');
    var viewsViewSelect = $('#views_view_select');
    var viewsList = $('#views_list');
    var viewDetails = $('#view_details');
    var viewCode = $('#viewcode');
    var viewResultsBlock = $('#view_results_block');
    var previewRandomDoc = $('#preview_random_doc');
    var viewCodeErrors = $('#view_code_errors');
    var spatialCodeErrors = $('#spatial_code_errors');
    var spatialCode = $('#spatialcode');
    var spatialResultBlock = $('#spatial_results_block');
    var whenInsideView = $('.when-inside-view', views);
    var justSaveView = $('#just_save_view');
    var saveViewAs = $('#save_view_as');
    var saveSpatialAs = $('#save_spatial_as');
    var justSaveSpatial = $('#just_save_spatial');
    var runBtn = $('.run_button',  views);

    var viewResultsContainer = $('#view_results_container');
    var viewResultsSpinner = $('#view_results_spinner');

    self.canCreateDDocCell = Cell.compute(function (v) {
      return (v.need(DAL.cells.mode) == 'views')
        && !!v(self.viewsBucketCell)
        && !v.need(self.ddocsAreInFactMissingCell);
    }).name("canCreateDDocCell");
    self.canCreateDDocCell.subscribeValue(function (val) {
      btnCreate[val ? 'removeClass' : 'addClass']('disabled');
    });

    Cell.subscribeMultipleValues(function (sec, tabsVal, spatialVal, viewsVal, subset, intPageFull, intPage, builder) {
      if (sec !== 'views') {
        return;
      }

      if (!tabsVal) {
        return;
      }

      if (!spatialVal) {
        var filterParams = "bbox=-180,-90,180,90";
        filterParams += tabsVal === 'production' ? "&stale=update_after&connection_timeout=60000" : "&connection_timeout=60000";
        SpatialFilter.rawFilterParamsCell.setValue(filterParams);
      }

      if (!viewsVal) {
        ViewsFilter.rawFilterParamsCell.setValue(tabsVal === 'production' ? "stale=update_after&connection_timeout=60000" : "connection_timeout=60000");
      }

      var intPageVal = subset === 'prod' ? intPageFull : intPage;

      if (builder) {
        var url = builder(intPageVal, subset);
        url = url.replace(/^http:\/\/(.*?):/, "http://" + window.location.hostname + ":");
        var text = url.substring(url.indexOf('?'));
        ViewsFilter.filtersUrl.attr('href', url);
        ViewsFilter.filtersUrl.text(text);
        SpatialFilter.filtersUrl.attr('href', url);
        SpatialFilter.filtersUrl.text(text);
      }
    }, DAL.cells.mode, self.modeTabs, SpatialFilter.rawFilterParamsCell,
         ViewsFilter.rawFilterParamsCell, self.subsetTabCell, self.intFullSubsetPageCell, self.intPageCell, self.proposedURLBuilderCell);

    viewsBucketSelect.bindListCell(self.populateBucketsDropboxCell, {
      onChange: function (e, newValue) {
        self.rawViewNameCell.setValue(undefined);
        self.rawDDocIdCell.setValue(undefined);
        self.rawViewNameCell.setValue(undefined);
        self.rawViewsBucketCell.setValue(newValue);
      }
    });

    btnCreate.bind('click', function (e) {
      e.preventDefault();
      ViewsSection.startCreateView();
    });

    viewsViewSelect.bindListCell(self.populateViewsDropboxCell, {
      onChange: function (e, newValue) {
        if (!newValue) {
          self.rawDDocIdCell.setValue(undefined);
          self.rawViewNameCell.setValue(undefined);
          self.rawSpatialNameCell.setValue(undefined);
          return;
        }
        unbuildViewPseudoLink(newValue, function (_ignored, ddocId, type, viewName) {
          var devMode = isDevModeDoc({meta: {id: ddocId}});
          _.defer(function () {
            viewsViewSelect.parent().find('.selectBox-label')
              .html(escapeHTML(ddocId).split('/').join('/<strong>') +
                '</strong>/' + type +
                '/<strong>' + escapeHTML(viewName) + '</strong>');
            self.modeTabs.setValue(devMode ? 'development' : 'production');
            if (type === '_view') {
              self.setCurrentView(ddocId, viewName);
            } else {
              self.setCurrentSpatial(ddocId, viewName);
            }
          });
        });
      },
      applyWidget: function () {},
      unapplyWidget: function () {},
      buildOptions: function (q, selected, list) {
        _.each(list, function (group) {
          var option = $(group);
          q.append(option);
        });
        viewsViewSelect.selectBox({autoWidth:false})
          .selectBox('options', list.join(''));
        if (selected.length > 0) {
          viewsViewSelect.parent().find('.selectBox-label')
            .html(escapeHTML(selected[0].meta.id).split('/').join('/<strong>') + '</strong>/_view/<strong>' + escapeHTML(selected[1]) + '</strong>');
        }
      }
    });

    DAL.subscribeWhenSection(self.currentDDocAndView, "views", function (value) {
      // only run it if the current clicked view isn't a spatial one
      if (self.currentDDocAndSpatial.value && self.currentDDocAndSpatial.value.length == 0) {
        viewsList[(value && !value.length) ? 'show' : 'hide']();
        viewDetails[(value && value.length) ? 'show' : 'hide']();
      }
      viewCode[(value && value.length) ? 'show' : 'hide']();
      viewResultsBlock[(value && value.length) ? 'show' : 'hide']();
      if (value && value.length) {
        previewRandomDoc.trigger('click', true);
        self.mapEditor.refresh();
        self.reduceEditor.refresh();
      }
    });

    DAL.subscribeWhenSection(self.currentDDocAndSpatial, "views", function (value) {
      // only run it if the current clicked view isn't a non-spatial one
      if (self.currentDDocAndView.value && self.currentDDocAndView.value.length == 0) {
        viewsList[(value && !value.length) ? 'show' : 'hide']();
        viewDetails[(value && value.length) ? 'show' : 'hide']();
      }
      spatialCode[(value && value.length) ? 'show' : 'hide']();
      spatialResultBlock[(value && value.length) ? 'show' : 'hide']();
      if (value && value.length) {
        previewRandomDoc.trigger('click', true);
        self.spatialEditor.refresh();
      }
    });

    (function () {
      var originalMap, originalReduce, originalSpatial;

      Cell.subscribeMultipleValues(function (view, spatial) {
        whenInsideView[view || spatial ? 'show' : 'hide']();

        if (view !== undefined) {
          originalMap = view.map;
          originalReduce = view.reduce || "";
          viewCodeErrors.text('').attr('title','');
          // NOTE: this triggers onChange right now so it needs to be last
          self.mapEditor.setValue(view.map);
          self.reduceEditor.setValue(view.reduce || "");
        } else if (spatial !== undefined) {
          originalSpatial = spatial;
          spatialCodeErrors.text('').attr('title','');

          // NOTE: this triggers onChange right now so it needs to be last
          self.spatialEditor.setValue(spatial);
        }
      }, self.currentView, self.currentSpatial);

      var unchangedCell = new Cell();

      var onMapReduceChange = function () {
        var nowMap = self.mapEditor.getValue();
        var nowReduce = self.reduceEditor.getValue();
        var nowSpatial = self.spatialEditor.getValue();
        var unchanged = (
          (originalMap === undefined || originalMap === nowMap) &&
          (originalReduce === undefined || originalReduce === nowReduce) &&
          (originalSpatial === undefined || originalSpatial === nowSpatial));
        unchangedCell.setValue(unchanged);
      };

      self.mapEditor.setOption('onChange', onMapReduceChange);
      self.reduceEditor.setOption('onChange', onMapReduceChange);
      self.spatialEditor.setOption('onChange', onMapReduceChange);
      unchangedCell.subscribeValue(function (unchanged) {
        runBtn.toggleClass('disabled', !unchanged);
      });

    })();

    function enableEditor(editor) {
      editor.setOption('readOnly', false);
      editor.setOption('matchBrackets', true);
      $(editor.getWrapperElement()).removeClass('read_only');
    }

    function disableEditor(editor) {
      editor.setOption('readOnly', 'nocursor');
      editor.setOption('matchBrackets', false);
      $(editor.getWrapperElement())
        .addClass('read_only')
        .closest('.shadow_box').removeClass('editing');
    }

    self.editingDevView.subscribeValue(function (devView) {

      justSaveView[devView ? "show" : "hide"]();
      saveViewAs[devView ? "show" : "hide"]();

      if (!devView) {
        disableEditor(self.mapEditor);
        disableEditor(self.reduceEditor);
      } else {
        enableEditor(self.mapEditor);
        enableEditor(self.reduceEditor);
      }
    });

    self.editingDevViewSpatial.subscribeValue(function (devView) {

      justSaveSpatial[devView ? "show" : "hide"]();
      saveSpatialAs[devView ? "show" : "hide"]();

      if (!devView) {
        disableEditor(self.spatialEditor);
      } else {
        enableEditor(self.spatialEditor);
      }
    });

    (function () {
      self.viewResultsURLCell.runView = function () {
        var urlCell = self.defaultSubsetResultsURLCell;
        var appliedBuilderCell = self.defaultSubsetAppliedURLBuilderCell;
        if (self.subsetTabCell.value === "prod") {
          urlCell = self.fullSubsetResultsURLCell;
          appliedBuilderCell = self.fullSubsetAppliedURLBuilderCell;
        }

        urlCell.setValue(undefined);
        Cell.waitQuiescence(function () {
          self.proposedURLBuilderCell.getValue(function (value) {
            appliedBuilderCell.setValue(value);
            self.lastAppliedURLBuilderCell.setValue(value);
            urlCell.recalculate();
          });
        });
      };

      self.proposedURLBuilderCell.subscribeValue(function (proposed) {
        if (proposed !== self.lastAppliedURLBuilderCell.value) {
          self.subsetTabCell.setValue("dev");
          self.pageNumberCell.setValue(undefined);
          self.fullSubsetPageNumberCell.setValue(undefined);
          self.lastAppliedURLBuilderCell.setValue(undefined);
          self.defaultSubsetAppliedURLBuilderCell.setValue(undefined);
          self.fullSubsetAppliedURLBuilderCell.setValue(undefined);
        }
      });
    })();

    self.viewResultsMassagedForTemplateCell.subscribeValue(function (value) {
      if (!value) {
        return;
      }
      renderTemplate('view_results', value);
      $('.sample-document-link').click(function (e) {
        e.preventDefault();
        self.sampleDocumentIdCell.setValue($(this).attr('data-sample-doc-id'));
      });
    });

    self.viewResultsCellSpatial.subscribeValue(function (value) {
      if (value) {
        var rows = _.filter(value.rows, function (r) {return ('bbox' in r)});
        var targ = {rows: rows};
      } else {
        var targ = {rows: {lackOfValue: true}};
      }
      renderTemplate('spatial_results', targ);
    });

    (function () {
      var resultBlock = $('.results_block', views);
      var prevBtn = $('.arr_prev', resultBlock);
      var nextBtn = $('.arr_next', resultBlock);
      var icPrevNext = $('.ic_prev_next', resultBlock);

      DAL.subscribeWhenSection(self.showHidePrevNextCell, "views", function (args) {
        if (!args) {
          return;
        }
        (function (viewResults, intPage) {
          if (!viewResults) {
            icPrevNext.hide();
            return;
          }
          icPrevNext.show();
          prevBtn.toggleClass('disabled', intPage == 1);
          nextBtn.toggleClass('disabled', (viewResults.rows.length < ViewsSection.PAGE_LIMIT) || intPage == 10);
        }).apply(this, args);
      });

      function pageBtnClicker(btn, body) {
        btn.click(function (ev) {
          ev.preventDefault();
          if (btn.hasClass('disabled')) {
            return;
          }
          var cells = [self.intPageCell, self.pageNumberCell];
          if (self.subsetTabCell.value === "prod") {
            cells = [self.intFullSubsetPageCell, self.fullSubsetPageNumberCell];
          }
          cells[0].getValue(function (intPage) {
            body(intPage, cells[1]);
          });
        });
      }

      pageBtnClicker(prevBtn, function (intPage, cell) {
        if (intPage > 1) {
          cell.setValue((intPage-1).toString());
        }
      });

      pageBtnClicker(nextBtn, function (intPage, cell) {
        if (intPage < 10) {
          cell.setValue((intPage+1).toString());
        }
      });
    })();

    Cell.subscribeMultipleValues(function (url, results) {
      viewResultsContainer[(url && !results) ? 'hide' : 'show']();
      viewResultsSpinner[(url && !results) ? 'show' : 'hide']();
    }, self.viewResultsURLCell, self.viewResultsCell);

    saveViewAs.bind('click', $m(self, 'startViewSaveAs'));
    justSaveView.bind('click', $m(self, 'saveView'));
    saveSpatialAs.bind('click', $m(self, 'startSpatialSaveAs'));
    justSaveSpatial.bind('click', $m(self, 'saveSpatial'));

    self.productionDDocsCell.subscribeValue(function(v) {
      if (v !== undefined) {
        var prodViewCount = $('#prod_view_count');
        if (v.length > 0) {
          prodViewCount.html(v.length).parent().fadeIn('fast');
        } else {
          prodViewCount.parent().fadeOut('fast');
        }
      }
    });

    function subscribeViewsList(cell, containerId) {
      DAL.subscribeWhenSection(cell, "views", function (ddocs) {
        var container = $i(containerId);

        if (!ddocs) {
          renderTemplate('views_list', {loading: true}, container);
          return;
        }

        var poolDetails = ddocs.poolDetails;
        var bucketName = ddocs.bucketName;

        _.each(ddocs, function (x) {
          var task = (ddocs.tasks[x.meta.id] || [])[0];
          if (task) {
            x.progress = task.progress;
            x.taskType = task.type;
          }
        });

        _.each(ddocs, function (doc, i) {
            ddocs[i].isEmpty = _.isEmpty(doc.json.spatial) && _.isEmpty(doc.json.views);
        });

        renderTemplate('views_list', {
          rows: ddocs,
          bucketName: bucketName,
          loading: false
        }, container);

        $('#development_views_list_container .list_button').click(function (e) {
          if ($(this).hasClass('disabled')) {
            e.stopImmediatePropagation();
            e.preventDefault();
          }
        });

      });
    }

    subscribeViewsList(self.massagedDevDDocsCell, 'development_views_list_container');
    subscribeViewsList(self.massagedProductionDDocsCell, 'production_views_list_container');

    var builtInReducers = $('#built_in_reducers a');
    var viewRunButton = $('#view_run_button');
    var spatialRunButton = $('#spatial_run_button');
    var sampleDocsCont = $('#sample_docs');
    var noSampleDocsCont = $('#no_sample_docs');
    var lookupDocById = $('#lookup_doc_by_id');
    var lookupDocByIdBtn = $("#lookup_doc_by_id_btn");
    var sampleMeta = $("#sample_meta");
    var docsTitle = $('.docs_title', views);
    var editDocument = $('#edit_sample_doc', views);

    builtInReducers.bind('click', function (e) {
      var text = $(this).text();
      if (viewcodeReduce.prop('disabled')) {
        return;
      }
      self.reduceEditor.setValue(text || "");
    });

    viewRunButton.bind('click', function (e) {
      if ($(this).hasClass('disabled')) {
        return;
      }
      e.preventDefault();
      self.runCurrentView();
    });
    spatialRunButton.bind('click', function (e) {
      if ($(this).hasClass('disabled')) {
        return;
      }
      e.preventDefault();
      self.runCurrentSpatial();
    });
    editDocument.bind('click', function (e) {
      if ($(this).hasClass('disabled')) {
        e.preventDefault();
        return;
      }
    });

    var selectedBucket;

    self.viewsBucketCell.subscribeValue(function (selected) {
      selectedBucket = selected;
    });

    previewRandomDoc.click(function (ev) {
      self.sampleDocumentIdCell.setValue(undefined);
      self.sampleDocCell.setValue(undefined);
      self.sampleDocCell.invalidate();
    });

    self.sampleDocCell.subscribeValue(function (doc) {
      editDocument.removeClass('disabled');
      if (doc) {
        sampleDocsCont.show();
        noSampleDocsCont.hide();

        var id = doc.meta.id;

        docsTitle.text(id);

        var param = {};
        param.data = doc;
        param.loading = false;

        sampleMeta.text(JSON.stringify(doc.meta, null, "\t"));
        renderTemplate('sample_documents', param);
        editDocument.attr('href', '#sec=documents&bucketName=' + encodeURIComponent(selectedBucket) + '&docId=' + encodeURIComponent(doc.meta.id));
      } else {
        editDocument.addClass('disabled');
        if (doc === null) {
          sampleDocsCont.hide();
          noSampleDocsCont.show();
        } else {
          docsTitle.text('Sample Document');
          sampleMeta.text("");
          renderTemplate('sample_documents', {loading: true});
        }
      }
    });
  },
  doDeleteDDoc: function (url, callback) {
    begin();
    function begin() {
      couchGet(url, withDoc);
    }
    function withDoc(ddoc) {
      if (!ddoc) {
        // 404
        callback(ddoc);
        return;
      }
      couchReq('DELETE',
               url + "?" + $.param({rev: ddoc.meta.rev}),
               {},
               function () {
                 callback(ddoc);
               },
               function (error, status, handleUnexpected) {
                 if (status == 409) {
                   return begin();
                 }
                 return handleUnexpected();
               }
              );
    }
  },
  doSaveAs: function (dbURL, ddoc, toId, overwriteConfirmed, callback) {
    var toURL = buildDocURL(dbURL, toId);
    return begin();

    function begin() {
      couchGet(toURL, withDoc);
    }
    function withDoc(toDoc) {
      if (toDoc && !overwriteConfirmed) {
        return callback("conflict");
      }
      couchReq('PUT',
               toURL,
               ddoc.json,
               function () {
                 callback("ok");
               },
               function (error, status, unexpected) {
                 if (status == 409) {
                   if (overwriteConfirmed) {
                     return begin();
                   }
                   return callback("conflict");
                 }
                 return unexpected();
               }, {
                 timeout: 60000
               });
    }
  },
  withBucketAndDDoc: function (bucketName, ddocId, body) {
    var self = this;
    self.ensureBucketSelected(bucketName, function () {
      self.withDDoc(ddocId, body);
    });
  },
  withDDoc: function (id, body) {
    ThePage.ensureSection("views");
    var allDDocs;
    var dbURL;

    this.allDDocsCell.getValue(function (val) {
      allDDocs = val;
    });
    this.dbURLCell.getValue(function (val) {
      dbURL = val;
    });

    var ddoc = _.detect(allDDocs, function (d) {return d.meta.id == id});
    var rv;

    if (ddoc) {
      rv = [buildDocURL(dbURL, ddoc.meta.id), ddoc, dbURL];
    } else {
      rv = [];
      console.log("ddoc with id:", id, " not found");
    }

    body.apply(null, rv);
  },
  startDDocDelete: function (id) {
    this.withDDoc(id, function (ddocURL, ddoc) {
      if (!ddocURL) // no such doc
        return;

      showDialog('delete_designdoc_confirmation_dialog', {
        eventBindings: [['.save_button', 'click', function (e) {
          e.preventDefault();
          ViewsSection.allDDocsCell.setValue(undefined);
          var spinner = overlayWithSpinner("#delete_designdoc_confirmation_dialog");
          var action = new ModalAction();

          ViewsSection.doDeleteDDoc(ddocURL, function () {
            spinner.remove();
            action.finish();
            ViewsSection.allDDocsCell.recalculate();
            hideDialog('delete_designdoc_confirmation_dialog');
          });
        }]]
      });
    });
  },
  cutOffDesignPrefix: function (id) {
    return id.replace(/^_design\/(dev_|)/, "");
  },
  maybeDisabledCompactButtonAttr: function (ddoc) {
    var can = (ddoc.taskType != 'view_compaction') && RecentlyCompacted.instance().canCompact(ddoc.compactURI);
    return can ? "" : "disabled";
  },
  compactDDoc: function (id, button) {
    var btn = $(button);
    if (btn.is('.disabled')) {
      return;
    }
    this.withDDoc(id, function (ddocURL, ddoc, dbURL) {
      var compactURI = ddoc.compactURI;
      btn.addClass('disabled');
      RecentlyCompacted.instance().registerAsTriggered(compactURI, function () {
        if (!btn.closest('body').length) {
          // btn is removed already
          return;
        }
        btn.removeClass('disabled');
      });
      $.ajax({
        url: compactURI,
        type: 'POST',
        success: function () {},
        error: function () {}
      });
    });
  },
  startDDocCopy: function (id) {
    this.withDDoc(id, function (ddocURL, ddoc, dbURL) {
      if (!ddocURL)
        return;

      var dialog = $('#copy_designdoc_dialog');
      var form = dialog.find("form");

      (function () {
        var name = ViewsSection.cutOffDesignPrefix(ddoc.meta.id);

        setFormValues(form, {
          "ddoc_name": name
        });
      })();

      showDialog(dialog, {
        eventBindings: [['.save_button', 'click', function (e) {
          e.preventDefault();
          var data = $.deparam(serializeForm(form));
          var toId = "_design/dev_" + data.ddoc_name;
          var spinner = overlayWithSpinner($(dialog));
          var needReload = false;

          function closeDialog() {
            spinner.remove();
            hideDialog(dialog);
            if (needReload) {
              ViewsSection.allDDocsCell.recalculate();
              ViewsSection.modeTabs.setValue('development');
            }
          }

          loop(false);

          function loop(overwriteConfirmed) {
            var modal = new ModalAction();
            ViewsSection.doSaveAs(dbURL, ddoc, toId, overwriteConfirmed, function (arg) {
              modal.finish();
              if (arg == "conflict") {
                genericDialog({text: "Please, confirm overwriting target Design Document.",
                               callback: function (e, name, instance) {
                                 instance.close();
                                 if (name != 'ok') {
                                   return closeDialog();
                                 }
                                 loop(true);
                               }});
                return;
              }
              if (arg != 'ok') BUG();
              needReload = true;
              closeDialog();
            });
          }
        }]]
      });
    });
  },
  doViewSave: function (dbURL, ddocId, viewName, viewDef, callback) {
    var ddocURL = buildDocURL(dbURL, ddocId);
    var overwriteConfirmed;
    var viewsLimitConfirmed;

    return begin();
    function begin() {
      couchGet(ddocURL, withDoc);
    }
    function withDoc(ddoc) {
      if (!ddoc) {
        ddoc = {meta: {id: ddocId},
                json: {views: {}}};
      }
      var views = ddoc.json.views || (ddoc.json.views = {});
      if (views[viewName] && !overwriteConfirmed) {
        return callback("error", "already_exists", "View with given name already exists", function () {
          overwriteConfirmed = true;
          begin();
        });
      } else if (!viewsLimitConfirmed && _.keys(views).length >= 10) {
        return callback("error", "confirm_views_limit", undefined, function () {
          viewsLimitConfirmed = true;
          begin();
        });
      }
      views[viewName] = viewDef || {
        map:'function (doc, meta) {'
          + '\n  emit(meta.id, null);'
          + '\n}'
      }
      $('#view_code_errors').text('').attr('title','');
      couchReq('PUT',
               ddocURL,
               ddoc.json,
               function () {
                 callback("ok");
               },
               function (error, status, unexpected) {
                 if (status == 409) {
                   return begin();
                 }
                 if (status == 400) {
                   callback("error", error.error, error.reason, function () {
                     BUG("this is not expected");
                   });
                   return;
                 }
                 return unexpected();
               });
    }
  },
  doSaveSpatial: function (dbURL, ddocId, spatialName, overwriteConfirmed,
      callback, spatialDef) {
    var ddocURL = buildDocURL(dbURL, ddocId);
    return begin();
    function begin() {
      couchGet(ddocURL, withDoc);
    }
    function withDoc(ddoc) {
      if (!ddoc) {
          ddoc = {meta: {id: ddocId},
                  json: {views: {}}};
      }
      var spatial = ddoc.json.spatial || (ddoc.json.spatial = {});
      if (spatial[spatialName] && !overwriteConfirmed) {
        return callback("conflict", "already_exists", "Spatial Function with given name already exists");
      }
      spatial[spatialName] = spatialDef ||
          "function (doc) {\n  if (doc.geometry) {\n    emit(doc.geometry, null);\n  }\n}";

      $('#spatial_code_errors').text('').attr('title','');

      couchReq('PUT',
               ddocURL,
               ddoc.json,
               function () {
                 callback("ok");
               },
               function (error, status, unexpected) {
                 if (status == 409) {
                   return begin();
                 }
                 if (status == 400) {
                   callback("error", error.error, error.reason, function () {
                     BUG("this is not expected");
                   });
                   return;
                 }
                 return unexpected();
               });
    }
  },
  startCreateView: function (ddocId) {
    var dbURL = ViewsSection.dbURLCell.value;
    if (!dbURL) {
      return;
    }
    var dialog = $('#copy_view_dialog');
    var warning = dialog.find('.warning').hide();
    dialog.find('[name=designdoc_name], [name=view_name]').val('');
    var ddocNameInput = dialog.find('[name=designdoc_name]').need(1);
    ddocNameInput.prop('disabled', !!ddocId);
    if (ddocId) {
      ddocNameInput.val(this.cutOffDesignPrefix(ddocId));
    }
    showDialog(dialog, {
      title: 'Create Development View',
      closeOnEscape: false,
      eventBindings: [['.save_button', 'click', function (e) {
        e.preventDefault();
        startSaving(ddocNameInput.val(), dialog.find('[name=view_name]').val());
      }]]
    });
    return;

    function startSaving(ddocName, viewName) {
      if (!ddocName || !viewName) {
        warning.text("Design Document and View names cannot be empty").show();
        return;
      }
      var modal = new ModalAction();
      var spinner = overlayWithSpinner(dialog);
      return ViewsSection.doViewSave(dbURL, "_design/dev_" + ddocName, viewName, undefined, saveCallback);

      function saveCallback(status, error, reason, continueSaving) {
        if (error == 'confirm_views_limit') {
          return ViewsSection.displayViewsLimitConfirmation(function (e, name, instance) {
            modal.finish();
            instance.close();
            if (name == 'ok') {
              modal = new ModalAction();
              return continueSaving();
            }
            spinner.remove();
          });
        }

        var closeDialog = false;
        if (status != "ok") {
          warning.text(reason);
          warning.show();
        } else {
          closeDialog = true;
        }
        modal.finish();
        spinner.remove();
        if (closeDialog) {
          ddocNameInput.prop('disabled', false);
          hideDialog(dialog);
          ViewsSection.allDDocsCell.recalculate();
          ViewsSection.modeTabs.setValue("development");
        }
      }
    }
  },
  doRemoveView: function (ddocURL, viewName, callback) {
    return begin();
    function begin() {
      couchGet(ddocURL, withDoc);
    }
    function withDoc(ddoc) {
      if (!ddoc) {
        return;
      }
      var views = ddoc.json.views || (ddoc.json.views = {});
      if (!views[viewName]) {
        return;
      }
      delete views[viewName];
      couchReq('PUT',
               ddocURL,
               ddoc.json,
               function () {
                 callback();
               },
               function (error, status, unexpected) {
                 if (status == 409) {
                   return begin();
                 }
                 return unexpected();
               });
    }
  },
  startRemoveView: function (pseudoLink) {
    return unbuildViewPseudoLink(pseudoLink, this.doStartRemoveView, this);
  },
  doStartRemoveView: function (bucketName, ddocId, type, viewName) {
    var self = this;
    self.withBucketAndDDoc(bucketName, ddocId, function (ddocURL, ddoc) {
      genericDialog({text: "Deleting this view will result in the index " +
        "for this Design Document to be regenerated when next requested. " +
        "Are you sure you want to delete this view from this Design Document?",
                     callback: function (e, name, instance) {
                       instance.close();
                       if (name === 'ok') {
                         performRemove();
                       }
                     }});

      function performRemove() {
        self.allDDocsCell.setValue(undefined);
        self.doRemoveView(ddocURL, viewName, function () {
          self.allDDocsCell.recalculate();
        });
      }
    });
  },
  startViewSaveAs: function () {
    var self = this;
    var dialog = $('#copy_view_dialog');
    var warning = dialog.find('.warning').hide();

    self.rawForDoSaveAsView.getValue(function (args) {
      begin.apply(self, args);
    });

    return;

    function begin(dbURL, ddoc, viewName) {
      if (viewName == null) {
        return;
      }

      var form = dialog.find('form');

      setFormValues(form, {
        'designdoc_name': self.cutOffDesignPrefix(ddoc.meta.id),
        'view_name': viewName
      });

      showDialog(dialog, {
        eventBindings: [['.save_button', 'click', function (e) {
          e.preventDefault();
          var params = $.deparam(serializeForm(form));
          startSaving(dbURL, ddoc, viewName, params['designdoc_name'], params['view_name']);
        }]]
      });
    }

    var spinner;
    var modal;

    function startSaving(dbURL, ddoc, viewName, newDDocName, newViewName) {
      if (!newDDocName || !newViewName) {
        warning.text('Both Design Document name and view name need to be specified').show();
        return;
      }

      spinner = overlayWithSpinner(dialog);
      modal = new ModalAction();

      var newId = "_design/dev_" + newDDocName;

      var mapCode = self.mapEditor.getValue();
      var reduceCode = self.reduceEditor.getValue();

      var view = ddoc.json.views[viewName];
      if (!view) BUG();
      view = _.clone(view);
      view.map = mapCode;
      if (reduceCode) {
        view.reduce = reduceCode;
      } else {
        delete view.reduce;
      }

      saveView(dbURL, newId, newViewName, view, ddoc.meta.id === newId && newViewName === viewName);
    }

    function saveView(dbURL, ddocId, viewName, view, sameDoc) {
      return ViewsSection.doViewSave(dbURL, ddocId, viewName, view, callback);

      function callback(arg, error, reason, continueSaving) {
        if (arg === 'ok') {
          return saveSucceeded(ddocId, viewName);
        }

        if (error == 'already_exists') {
          if (sameDoc) {
            return continueSaving();
          }
          return genericDialog({text: "Please, confirm overwriting exiting view.",
                                callback: function (e, name, instance) {
                                  if (name == 'ok') {
                                    return continueSaving();
                                  }
                                  modal.finish();
                                  spinner.remove();
                                }});
        }

        if (error == 'confirm_views_limit') {
          return self.displayViewsLimitConfirmation(function (e, name, instance) {
            modal.finish();
            instance.close();
            if (name == 'ok') {
              modal = new ModalAction();
              return continueSaving();
            }
            spinner.remove();
          });
        }

        modal.finish();
        spinner.remove();
        warning.show().text(reason);
      }
    }

    function saveSucceeded(ddocId, viewName) {
      modal.finish();
      spinner.remove();
      hideDialog(dialog);
      self.rawDDocIdCell.setValue(ddocId);
      self.rawViewNameCell.setValue(viewName);
      self.allDDocsCell.recalculate();
    }
  },
  displayViewsLimitConfirmation: function (callback) {
    genericDialog({text: "Creating more than 10 views per design document may decrease performance. Please, confirm.",
                   callback: callback});
  },
  saveView: function () {
    var self = this;
    var dialog = $('#copy_view_dialog');
    var warning = dialog.find('.warning').hide();

    self.rawForDoSaveView.getValue(function (args) {
      begin.apply(self, args);
    });

    return;

    function begin(dbURL, currentView, ddoc, viewName) {
      var mapCode = self.mapEditor.getValue();
      var reduceCode = self.reduceEditor.getValue();
      var changed = (currentView.map !== mapCode) || ((currentView.reduce || "") !== reduceCode);

      currentView = _.clone(currentView);
      currentView.map = mapCode;
      if (reduceCode) {
        currentView.reduce = reduceCode;
      } else {
        delete currentView.reduce;
      }
      return ViewsSection.doViewSave(dbURL, ddoc.meta.id, viewName, currentView, saveCallback);

      function saveCallback(status, error, reason, continueSaving) {
        if (error === 'confirm_views_limit' || error === 'already_exists') {
          return continueSaving();
        }

        if (status !== "ok") {
          $('#view_code_errors').text(reason).attr('title', reason);
        } else {
          self.allDDocsCell.recalculate();
        }
      }
    }
  },
  startCreateSpatial: function (ddocId) {
    var dbURL = ViewsSection.dbURLCell.value;
    if (!dbURL) {
      return;
    }
    var dialog = $('#copy_view_dialog');
    var warning = dialog.find('.warning').hide();
    dialog.find('[name=designdoc_name], [name=view_name]').val('');
    var ddocNameInput = dialog.find('[name=designdoc_name]').need(1);
    ddocNameInput.prop('disabled', !!ddocId);
    if (ddocId) {
      ddocNameInput.val(this.cutOffDesignPrefix(ddocId));
    }
    showDialog(dialog, {
      title: 'Create Development Spatial Function',
      closeOnEscape: false,
      eventBindings: [['.save_button', 'click', function (e) {
        e.preventDefault();
        startSaving(ddocNameInput.val(), dialog.find('[name=view_name]').val());
      }]]
    });

    function startSaving(ddocName, spatialName) {
      if (!ddocName || !spatialName) {
        warning.text("Design Document and Spatial Function names cannot be empty").show();
        return;
      }
      // TODO: maybe other validation
      var modal = new ModalAction();
      var spinner = overlayWithSpinner(dialog);
      ViewsSection.doSaveSpatial(dbURL, "_design/dev_" + ddocName, spatialName, false, function (status, error, reason) {
        var closeDialog = false;
        if (status == "conflict") {
          warning.text(reason);
          warning.show();
        } else {
          closeDialog = true;
        }
        modal.finish();
        spinner.remove();
        if (closeDialog) {
          ddocNameInput.prop('disabled', false);
          hideDialog(dialog);
          ViewsSection.allDDocsCell.recalculate();
          ViewsSection.modeTabs.setValue("development");
        }
      });
    }
  },
  doRemoveSpatial: function (ddocURL, spatialName, callback) {
    return begin();
    function begin() {
      couchGet(ddocURL, withDoc);
    }
    function withDoc(ddoc) {
      if (!ddoc) {
        return;
      }
      var spatial = ddoc.json.spatial || (ddoc.json.spatial = {});
      if (!spatial[spatialName]) {
        return;
      }
      delete spatial[spatialName];
      couchReq('PUT',
               ddocURL,
               ddoc.json,
               function () {
                 callback();
               },
               function (error, status, unexpected) {
                 if (status == 409) {
                   return begin();
                 }
                 return unexpected();
               });
    }
  },
  startRemoveSpatial: function (pseudoLink) {
    return unbuildViewPseudoLink(pseudoLink, this.doStartRemoveSpatial, this);
  },
  doStartRemoveSpatial: function (bucketName, ddocId, type, spatialName) {
    var self = this;
    self.withBucketAndDDoc(bucketName, ddocId, function (ddocURL, ddoc) {
      genericDialog({text: "Deleting this Spatial Function will result in the index" +
        "for this Design Document to be regenerated when next requested. " +
        "Are you sure you want to delete this Spatial Function from this Design Document?",
                     callback: function (e, name, instance) {
                       instance.close();
                       if (name === 'ok') {
                         performRemove();
                       }
                     }});

      function performRemove() {
        self.allDDocsCell.setValue(undefined);
        self.doRemoveSpatial(ddocURL, spatialName, function () {
          self.allDDocsCell.recalculate();
        });
      }
    });
  },
  startSpatialSaveAs: function () {
    var self = this;
    var dialog = $('#copy_view_dialog');
    var warning = dialog.find('.warning').hide();

    self.rawForSpatialDoSaveAs.getValue(function (args) {
      begin.apply(self, args);
    });

    return;

    function begin(dbURL, ddoc, spatialName) {
      if (spatialName == null) {
        return;
      }

      var form = dialog.find('form');

      setFormValues(form, {
        'designdoc_name': self.cutOffDesignPrefix(ddoc.meta.id),
        'view_name': spatialName
      });

      showDialog(dialog, {
        eventBindings: [['.save_button', 'click', function (e) {
          e.preventDefault();
          var params = $.deparam(serializeForm(form));
          startSaving(dbURL, ddoc, spatialName, params['designdoc_name'], params['view_name']);
        }]]
      });
    }

    var spinner;
    var modal;

    function startSaving(dbURL, ddoc, spatialName, newDDocName, newSpatialName) {
      if (!newDDocName || !newSpatialName) {
        warning.text('Both Design Document name and Spatial Function name need to be specified').show();
        return;
      }

      spinner = overlayWithSpinner(dialog);
      modal = new ModalAction();

      var newId = "_design/dev_" + newDDocName;
      var spatialCode = self.spatialEditor.getValue();

      doSaveSpatial(dbURL, newId, newSpatialName, spatialCode, ddoc.meta.id === newId && newSpatialName === spatialName);
    }

    function doSaveSpatial(dbURL, ddocId, spatialName, spatial, overwriteConfirmed) {
      return ViewsSection.doSaveSpatial(dbURL, ddocId, spatialName, overwriteConfirmed, callback, spatial);

      function callback(arg, error, reason) {
        if (arg === "conflict") {
          return confirmOverwrite(dbURL, ddocId, spatialName, spatial);
        }

        modal.finish();
        spinner.remove();

        if (arg == "error") {
          warning.show().text(reason);
        } else {
          saveSucceeded(ddocId, spatialName);
        }
      }
    }

    function confirmOverwrite(dbURL, ddocId, spatialName, spatial) {
      genericDialog({text: "Please, confirm overwriting exiting Spatial Function.",
                     callback: function (e, name, instance) {
                       if (name == 'ok') {
                         return doSaveSpatial(dbURL, ddocId, spatialName, spatial, true);
                       }
                       modal.finish();
                       spinner.remove();
                     }});
    }

    function saveSucceeded(ddocId, spatialName) {
      hideDialog(dialog);
      self.rawDDocIdCell.setValue(ddocId);
      self.rawSpatialNameCell.setValue(spatialName);
      self.allDDocsCell.recalculate();
    }
  },

  saveSpatial: function () {
    var self = this;
    var dialog = $('#copy_view_dialog');
    var warning = dialog.find('.warning').hide();

    self.rawForSpatialDoSave.getValue(function (args) {
      begin.apply(self, args);
    });

    return;

    function begin(dbURL, currentSpatial, ddoc, spatialName) {
      var spatialCode = self.spatialEditor.getValue();
      var changed = (currentSpatial !== spatialCode);

      currentSpatial = _.clone(currentSpatial);
      currentSpatial = spatialCode;
      return ViewsSection.doSaveSpatial(dbURL, ddoc.meta.id, spatialName, true,
        saveCallback, currentSpatial);

      function saveCallback(status, error, reason, continueSaving) {
        if (error === 'confirm_views_limit' || error === 'already_exists') {
          return continueSaving();
        }

        if (status !== "ok") {
          $('#spatial_code_errors').text(reason).attr('title', reason);
        } else {
          self.allDDocsCell.recalculate();
        }
      }
    }
  },
  runCurrentSpatial: function () {
    SpatialFilter.closeFilter();
    this.viewResultsURLCell.runView();
  },
  runCurrentView: function () {
    ViewsFilter.closeFilter();
    this.viewResultsURLCell.runView();
  },
  showView: function (plink) {
    return unbuildViewPseudoLink(plink, this.doShowView, this);
  },
  setCurrentView: function (ddocId, viewName) {
    var self = this;
    self.dbURLCell.getValue(function (dbURL) {
      self.rawDDocIdCell.setValue(ddocId);
      self.rawViewNameCell.setValue(viewName);
      self.rawSpatialNameCell.setValue(undefined);
    });
  },
  doShowView: function (bucketName, ddocId, type, viewName) {
    var self = this;
    self.withBucketAndDDoc(bucketName, ddocId, function (ddocURL, ddoc) {
      if (!ddoc) {
        return;
      }
      self.setCurrentView(ddoc.meta.id, viewName);
    });
  },
  startPublish: function (id) {
    var self = this;
    self.withDDoc(id, function (ddocURL, ddoc, dbURL) {
      if (!ddocURL) {
        return;
      }
      var name = self.cutOffDesignPrefix(ddoc.meta.id);
      var newId = "_design/" + name;
      var toURL = buildDocURL(dbURL, newId);

      publish();

      function publish(overwritten, callback) {
        self.doSaveAs(dbURL, ddoc, newId, overwritten, function (arg) {
          if (arg == "conflict") {
            return genericDialog({
              header: 'Confirm Publishing',
              text: 'Design Document with this name already exists, it will be overwritten.',
              buttons: {
                ok: "Confirm",
                cancel: true
              },
              callback: function (e, name, instance) {
                if (name != 'ok') {
                  instance.close();
                } else {
                  var modal = new ModalAction();
                  var spinner = overlayWithSpinner(instance.dialog.parent());
                  publish(true, function () {
                    spinner.remove();
                    modal.finish();
                    instance.close();
                  });
                }
             }});
          }
          if (callback) {
            callback();
          }
          if (arg == 'ok') {
            self.allDDocsCell.recalculate();
            self.modeTabs.setValue('production');
          } else {
            BUG();
          }
        });
      }
    });
  },
  showSpatial: function (plink) {
    return unbuildViewPseudoLink(plink, this.doShowSpatial, this);
  },
  setCurrentSpatial: function (ddocId, spatialName) {
    var self = this;
    self.dbURLCell.getValue(function (dbURL) {
      self.rawDDocIdCell.setValue(ddocId);
      self.rawViewNameCell.setValue(undefined);
      self.rawSpatialNameCell.setValue(spatialName);
    });
  },
  doShowSpatial: function (bucketName, ddocId, type, spatialName) {
    var self = this;
    self.withBucketAndDDoc(bucketName, ddocId, function (ddocURL, ddoc) {
      if (!ddoc) {
        return;
      }
      self.setCurrentSpatial(ddoc.meta.id, spatialName);
    });
  },
  onEnter: function () {
    this.allDDocsURLCell.setValue(undefined);
    this.allDDocsURLCell.recalculate();
    this.onLeave();
  },
  onLeave: function () {
    this.rawDDocIdCell.setValue(undefined);
    this.pageNumberCell.setValue(undefined);
    this.rawViewNameCell.setValue(undefined);
    this.rawSpatialNameCell.setValue(undefined);
    ViewsFilter.reset();
    SpatialFilter.reset();
  },
  navClick: function () {
    this.onLeave();
    this.onEnter();
  }
};

configureActionHashParam("deleteDDoc", $m(ViewsSection, "startDDocDelete"));
configureActionHashParam("copyDDoc", $m(ViewsSection, "startDDocCopy"));
configureActionHashParam("showView", $m(ViewsSection, "showView"));
configureActionHashParam("removeView", $m(ViewsSection, "startRemoveView"));
configureActionHashParam("publishDDoc", $m(ViewsSection, "startPublish"));
configureActionHashParam("addView", $m(ViewsSection, "startCreateView"));
configureActionHashParam("showSpatial", $m(ViewsSection, "showSpatial"));
configureActionHashParam("removeSpatial", $m(ViewsSection, "startRemoveSpatial"));
configureActionHashParam("addSpatial", $m(ViewsSection, "startCreateSpatial"));

var ViewsFilter = Filter({
  prefix: 'views',
  hashName: 'viewsFilter',
  container: $('#view_filter_container'),
  onClose: function () {
    ViewsSection.pageNumberCell.setValue(undefined);
    ViewsSection.fullSubsetPageNumberCell.setValue(undefined);
  }
}, {
  prefix: 'views',
  items: {
    all: true,
    conflict: false,
  }
});

var SpatialFilter = Filter({
  prefix: 'spatial',
  hashName: 'spatialFilter',
  container: $('#spatial_filter_container'),
  onClose: function () {
    ViewsSection.pageNumberCell.setValue(undefined);
    ViewsSection.fullSubsetPageNumberCell.setValue(undefined);
  }
}, {
  prefix: 'spatial',
  items: {
    stale: true,
    bbox: true,
    connectionTimeout: true
  }
});

$(function () {
  ViewsFilter.init();
  SpatialFilter.init();
});
