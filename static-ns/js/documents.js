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

 var documentErrors = {
  '502': '(The node containing that document is currently down)',
  '503': '(The data has not yet loaded, please wait...)',
  idEmpty: 'Document ID cannot be empty',
  notExist: '(Document does not exist)',
  unexpected: '(Unexpected error)',
  invalidJson: '(Document is invalid JSON)',
  alreadyExists: 'Document with given ID already exists',
  bucketNotExist: 'Bucket does not exist',
  bucketListEmpty: 'Buckets list empty'
 }

function createDocumentsCells(ns, modeCell, capiBaseCell, bucketsListCell) {
  //bucket
  ns.populateBucketsDropboxCell = Cell.compute(function (v) {
    var mode = v.need(modeCell);
    if (mode != 'documents') {
      return;
    }
    var buckets = v.need(bucketsListCell).byType.membase;
    var selectedBucketName = v.need(ns.selectedBucketCell);
    return {
      list: _.map(buckets, function (info) { return [info.name, info.name] }),
      selected: selectedBucketName
    };
  }).name("populateBucketsDropboxCell");

  ns.selectedBucketCell = Cell.compute(function (v) {
    var section = v.need(modeCell);
    if (section != 'documents') {
      return;
    }
    var selected = v(ns.bucketNameCell);
    var buckets = v.need(bucketsListCell).byType.membase;

    if (selected) {
      var bucketNotExist = !_.detect(buckets, function (info) { return info.name === selected });
      if (bucketNotExist) {
        return null;
      } else {
        return selected;
      }
    } else {
      var bucketInfo = _.detect(buckets, function (info) { return info.name === "default" }) || buckets[0];
      if (bucketInfo) {
        return bucketInfo.name;
      } else {
        return null;
      }
    }
  }).name("selectedBucketCell");
  ns.selectedBucketCell.equality = function (a, b) {return a === b;};

  ns.haveBucketsCell = Cell.compute(function (v) {
    return v.need(ns.selectedBucketCell) !== null;
  });

  //document
  ns.currentDocumentIdCell = Cell.compute(function (v) {
    var docId = v(ns.documentIdCell);
    if (docId) {
      return docId;
    }
    return null;
  }).name("currentDocumentIdCell");

  ns.currentDocURLCell = Cell.compute(function (v) {
    var currentDocId = v(ns.currentDocumentIdCell);
    if (currentDocId) {
      return buildDocURL(v.need(ns.dbURLCell), currentDocId);
    }
  }).name("currentDocURLCell");

  ns.rawCurrentDocCell = Cell.compute(function (v) {
    var dataCallback;

    return future.get({
      url: v.need(ns.currentDocURLCell),
      error: function (xhr) {
        var error = new Error(xhr.statusText);
        switch (xhr.status) {
          case '502':
            error.explanatoryMessage = documentErrors['502'];
            break;
          case '503':
            error.explanatoryMessage = documentErrors['503'];
            break;
          default:
            try {
              JSON.parse(xhr.responseText);
              error.explanatoryMessage = documentErrors.unexpected;
            } catch (jsonError) {
              jsonError.explanatoryMessage = documentErrors.invalidJson;
              dataCallback(jsonError);
              return;
            }
            break;
        }
        dataCallback(error);
      },
      success: function (doc, status, xhr) {
        if (doc) {
          var meta = xhr.getResponseHeader("X-Couchbase-Meta");
          dataCallback({
            json : doc,
            meta : JSON.parse(meta)
          });
        } else {
          var error = new Error(status);
          error.explanatoryMessage = documentErrors.notExist;
          dataCallback(error);
        }
      }}, undefined, undefined, function (initXHR) {
        return future(function (__dataCallback) {
          dataCallback = __dataCallback;
          initXHR(dataCallback);
        });
      });
  }).name('rawCurrentDocCell');

  ns.currentDocCell = Cell.compute(function (v) {
    if (v.need(ns.haveBucketsCell)) {
      return v.need(ns.rawCurrentDocCell);
    } else {
      var error = new Error('not found');
      error.explanatoryMessage = documentErrors.notExist;
      return error;
    }
  }).name("currentDocCell");

  ns.currentDocCell.delegateInvalidationMethods(ns.rawCurrentDocCell);

  //documents list
  ns.currentPageDocsURLCell = Cell.compute(function (v) {
    var currentDocId = v(ns.currentDocumentIdCell);
    if (currentDocId) {
      return;
    }
    var param = _.extend({}, v(ns.filter.filterParamsCell));
    var page = v.need(ns.currentDocumentsPageNumberCell);
    var limit = v(ns.currentPageLimitCell);
    var skip = page * limit;
    var url = v.need(ns.dbURLCell);

    param.skip = String(skip);
    param.include_docs = true;
    param.limit = String(limit + 1);

    return buildURL(url, "_all_docs", param);
  }).name("currentPageDocsURLCell");

  ns.currentPageDocsCell = Cell.compute(function (v) {
    if (v.need(ns.haveBucketsCell)) {
      return future.capiViewGet({url: v.need(ns.currentPageDocsURLCell)});
    } else {
      return {rows: []};
    }
  }).name("currentPageDocsCell");

  ns.dbURLCell = Cell.compute(function (v) {
    var base = v.need(capiBaseCell);
    var bucketName = v.need(ns.selectedBucketCell);

    if (bucketName) {
      return buildURL(base, bucketName) + "/";
    }
  }).name("dbURLCell");

  ns.currentPageLimitCell = Cell.compute(function (v) {
    var pageLimit = v(ns.pageLimitCell);
    if (pageLimit) {
      return Number(pageLimit);
    }
    return 5;
  });

  ns.currentDocumentsPageNumberCell = Cell.compute(function (v) {
    var page = v(ns.documentsPageNumberCell);
    if (!page) {
      return 0;
    }
    page = Number(page);
    var pageLimit = v.need(ns.currentPageLimitCell);
    var docsLimit = ns.docsLimit;
    var currentSkip = (page + 1) * pageLimit;

    if (currentSkip > docsLimit) {
      page = (docsLimit / pageLimit) - 1;
    }

    return page;
  }).name('currentDocumentsPageNumberCell');
}

var DocumentsSection = {
  docsLimit: 1000,
  init: function () {
    var self = this;

    self.bucketNameCell = new StringHashFragmentCell("bucketName");
    self.documentsPageNumberCell = new StringHashFragmentCell("documentsPageNumber");
    self.documentIdCell = new StringHashFragmentCell("docId");
    self.lookupIdCell = new StringHashFragmentCell("lookupId");
    self.pageLimitCell = new StringHashFragmentCell("documentsPageLimit");

    var documents = $('#documents');
    var allDocsCont = $('#documents_list', documents);
    var allDocsTitle = $('.docs_title', allDocsCont);

    self.filter = new Filter({
      prefix: 'documents',
      hashName: 'documentsFilter',
      appendTo: allDocsTitle,
      onClose: function (param) {
        if (param.startkey || param.endkey) {
          self.lookupIdCell.setValue(undefined);
        }
      }
    }, {
      title: 'Documents Filter',
      prefix: 'documents',
      items: {
        descending: true,
        inclusiveEnd: true,
        endkey: true,
        startkey: true
      }
    }).init();

    createDocumentsCells(self, DAL.cells.mode, DAL.cells.capiBase, DAL.cells.bucketsListCell);

    var createDocDialog = $('#create_document_dialog');
    var createDocWarning = $('.warning', createDocDialog);
    var createDocInput = $('#new_doc_id', createDocDialog);
    var deleteDocDialog = $("#delete_document_confirmation_dialog");
    var deleteDocDialogWarning = $('.error', deleteDocDialog);

    var breadCrumpDoc = $('#bread_crump_doc', documents);
    var prevNextCont  = $('.ic_prev_next', documents);
    var prevBtn = $('.arr_prev', documents);
    var nextBtn = $('.arr_next', documents);
    var currenDocCont = $('#documents_details', documents);
    var allDocsEditingNotice = $('.documents_notice', allDocsCont);
    var editingNotice = $('.documents_notice', currenDocCont);
    var jsonDocId = $('.docs_title', currenDocCont);
    var docsLookup = $('#docs_lookup_doc_by_id', documents);
    var docsLookupBtn = $('#docs_lookup_doc_by_id_btn', documents);
    var docDeleteBtn = $('#doc_delete', documents);
    var docSaveAsBtn = $('#doc_saveas', documents);
    var docSaveBtn = $('#doc_save', documents);
    var docCreateBtn = $('.btn_create', documents);
    var docsBucketsSelect = $('#docs_buckets_select', documents);
    var lookupDocForm = $('#search_doc', documents);
    var docsInfoCont = $('.docs_info', documents);
    var docsCrntPgCont = $('.docs_crnt_pg', documents);
    var itemsPerListWrap = $('.items_per_list_wrap', documents);
    var itemsPerList = $('select', itemsPerListWrap);
    var filterBtn = $('.filters_container', documents);

    itemsPerList.selectBox();

    self.jsonCodeEditor = CodeMirror.fromTextArea($("#json_doc")[0], {
      lineNumbers: true,
      matchBrackets: false,
      tabSize: 2,
      mode: { name: "javascript", json: true },
      theme: 'default',
      readOnly: 'nocursor',
      onChange: function (doc) {
        enableSaveBtn(false);
        var history = doc.historySize();
        if (history.redo == 0 && history.undo == 0) {
          return;
        }
        enableSaveBtn(true);
        onDocValueUpdate(doc.getValue());
      }
    });

    var documentsDetails = $('#documents_details');
    var codeMirror = $(".CodeMirror", documentsDetails);

    _.each([prevBtn, nextBtn, docsLookupBtn, docDeleteBtn, docSaveAsBtn, docSaveBtn, docCreateBtn], function (btn) {
      btn.click(function (e) {
        if ($(this).hasClass('disabled')) {
          e.stopImmediatePropagation();
        }
      })
    });

    function showDocumentListState(page) {
      prevBtn.toggleClass('disabled', page.pageNumber === 0);
      nextBtn.toggleClass('disabled', isLastPage(page));

      docsCrntPgCont.text(page.pageNumber + 1);

      var searchCriteria;
      self.filter.filterParamsCell.getValue(function (value) {
        searchCriteria = !$.isEmptyObject(value);
      });
      var firstRow = page.docs.rows[0];
      if (firstRow && firstRow.key instanceof Object) {
        var error = new Error(firstRow.key.error);
        error.explanatoryMessage = '(' + firstRow.key.reason + ')';
        showDocumetsListErrorState(error);
        renderTemplate('documents_list', {
          loading: false,
          rows: [],
          searchCriteria: searchCriteria,
          pageNumber: page.pageNumber,
          bucketName: page.bucketName
        });
      } else {
        if (page.docs.rows.length > page.pageLimit) {
          page.docs.rows.pop();
        }
        showDocumetsListErrorState(false);
        renderTemplate('documents_list', {
          loading: false,
          rows: page.docs.rows,
          searchCriteria: searchCriteria,
          pageNumber: page.pageNumber,
          bucketName: page.bucketName
        });

        $('.delete_btn', documents).click(function () {
          self.deleteDocument($(this).attr('data-id'));
        });
      }
    }

    function showDocumetsListErrorState(error) {
      var message = error ? buildErrorMessage(error) : '';
      allDocsEditingNotice.text(message);
      allDocsEditingNotice.attr('title', JSON.stringify(message));
      if (error) {
        self.documentsPageNumberCell.setValue(0);
      }
    }

    function isLastPage(page) {
      return  page.docs.rows.length <= page.pageLimit || page.pageLimit * (page.pageNumber + 1) === self.docsLimit;
    }

    function showCodeEditor(show) {
      self.jsonCodeEditor.setOption('readOnly', show ? false : 'nocursor');
      self.jsonCodeEditor.setOption('matchBrackets', show ? true : false);
      $(self.jsonCodeEditor.getWrapperElement())[show ? 'removeClass' : 'addClass']('read_only');
    }

    function showDocumentState(show) {
      showPrevNextCont(!show);
      showDocsInfoCont(!show);
      showItemsPerPage(!show);
      allDocsCont[show ? 'hide' : 'show']();
      currenDocCont[show ? 'show' : 'hide']();
      showCodeEditor(!show);
    }

    var documentSpinner;

    function showDocumentPendingState(show) {
      enableDeleteBtn(!show);
      enableSaveAsBtn(!show);
      editingNotice.text('');
      jsonDocId.text('');
      if (show) {
        if (!documentSpinner) {
          documentSpinner = overlayWithSpinner(codeMirror);
        }
      } else {
        if (documentSpinner) {
          documentSpinner.remove();
          documentSpinner = undefined;
        }
      }
    }

    function bucketExistsState(exists) {
      enableLookup(exists);
      enableDocCreateBtn(exists);
      enableFilterBtn(exists);
    }

    function enableSaveBtn(enable) {
      docSaveBtn[enable ? 'removeClass' : 'addClass']('disabled');
    }

    function enableSaveAsBtn(enable) {
      docSaveAsBtn[enable ? 'removeClass' : 'addClass']('disabled');
    }

    function enableLookup(enable) {
      docsLookup.attr({disabled: !enable});
      docsLookupBtn[enable ? 'removeClass' : 'addClass']('disabled');
    }

    function enableDocCreateBtn(enable) {
      docCreateBtn[enable ? 'removeClass' : 'addClass']('disabled');
    }

    function enableFilterBtn(enable) {
      filterBtn[enable ? 'removeClass' : 'addClass']('disabled');
    }

    function enableDeleteBtn(enable) {
      docDeleteBtn[enable ? 'removeClass' : 'addClass']('disabled');
    }

    function showPrevNextCont(show) {
      prevNextCont[show ? 'show' : 'hide']();
    }

    function showItemsPerPage(show) {
      itemsPerListWrap[show ? 'show' : 'hide']();
    }

    function showDocsInfoCont(show) {
      docsInfoCont[show ? 'show' : 'hide']();
    }

    function tryShowJson(obj) {
      var isError = obj instanceof Error;
      if (isError) {
        enableSaveBtn(false);
      }
      editingNotice.text(isError && obj ? buildErrorMessage(obj) : '');
      jsonDocId.text(isError ? '' : obj.meta.id);
      self.jsonCodeEditor.setValue(isError ? '' : JSON.stringify(obj.json, null, "  "));
      enableDeleteBtn(!isError);
      enableSaveAsBtn(!isError);
      showCodeEditor(!isError);
    }

    function onDocValueUpdate(json) {
      enableSaveAsBtn(true);
      editingNotice.text('');
      try {
        var parsedJSON = JSON.parse(json);
      } catch (error) {
        enableSaveBtn(false);
        enableSaveAsBtn(false);
        enableDeleteBtn(true);
        error.explanatoryMessage = documentErrors.invalidJson;
        editingNotice.text(buildErrorMessage(error));
        return false;
      }
      return parsedJSON;
    }

    function buildErrorMessage(error) {
      return error.name + ': ' + error.message + ' ' + error.explanatoryMessage;
    }

    (function () {
      var page = {};
      var prevPage;
      var nextPage;
      var afterPageLoad;

      Cell.subscribeMultipleValues(function (docs, currentPage, selectedBucket, pageLimit) {
        if (typeof currentPage === 'number') {
          prevPage = currentPage - 1;
          prevPage = prevPage < 0 ? 0 : prevPage;
          nextPage = currentPage + 1;
          page.pageLimit = pageLimit;
          page.pageNumber = currentPage;
        }
        if (docs) {
          page.docs = docs;
          page.bucketName = selectedBucket;
          showDocumentListState(page);
          if (afterPageLoad) {
            afterPageLoad();
          }
        } else {
          renderTemplate('documents_list', {loading: true});
          // we don't know total rows. that's why we can't allow user to quick clicks on next button
          nextBtn.toggleClass('disabled', true);
        }
      },
        self.currentPageDocsCell, self.currentDocumentsPageNumberCell, self.selectedBucketCell,
        self.currentPageLimitCell
      );

      self.currentPageLimitCell.subscribeValue(function (val) {
        itemsPerList.selectBox("value", val);
      });

      function prevNextCallback() {
        $('html, body').animate({scrollTop:0}, 250);
        afterPageLoad = undefined;
      }

      nextBtn.click(function (e) {
        self.documentsPageNumberCell.setValue(nextPage);
        afterPageLoad = prevNextCallback;
      });

      prevBtn.click(function (e) {
        self.documentsPageNumberCell.setValue(prevPage);
        afterPageLoad = prevNextCallback;
      });
    })();

    self.currentDocumentsPageNumberCell.subscribeValue(function (value) {
      docsCrntPgCont.text(value + 1);
    });

    self.lookupIdCell.subscribeValue(function (searchedDoc) {
      docsLookup.val(searchedDoc ? searchedDoc : '');
    });

    self.currentDocumentIdCell.subscribeValue(function (docId) {
      showDocumentState(!!docId);
      if (docId) {
        showDocumentPendingState(true);
      }
    });

    self.pageLimitCell.getValue(function (value) {
      itemsPerList.selectBox('value', value);
    });

    self.currentDocCell.subscribeValue(function (doc) {
      if (!doc) {
        return;
      }
      showDocumentPendingState(false);
      tryShowJson(doc);
    });

    self.selectedBucketCell.subscribeValue(function (selectedBucket) {
      if (selectedBucket === undefined) {
        return;
      }
      self.filter.reset();
      bucketExistsState(!!selectedBucket);
    });

    lookupDocForm.submit(function (e) {
      e.preventDefault();
      var docsLookupVal = $.trim(docsLookup.val());
      if (docsLookupVal) {
        self.documentIdCell.setValue(docsLookupVal);
      }
    });

    breadCrumpDoc.click(function (e) {
      e.preventDefault();
      self.documentIdCell.setValue(undefined);
      self.lookupIdCell.setValue(undefined);
      self.filter.rawFilterParamsCell.setValue(undefined);
    });

    itemsPerList.change(function (e) {
      self.pageLimitCell.setValue(Number($(this).val()));
      self.documentsPageNumberCell.setValue(0);
    });

    docsBucketsSelect.bindListCell(self.populateBucketsDropboxCell, {
      onChange: function (e, newValue) {
        self.bucketNameCell.setValue(newValue);
        self.documentsPageNumberCell.setValue(undefined);
        self.documentIdCell.setValue(undefined);
      },
      onRenderDone: function (input, args) {
        if (args.list.length == 0) {
          input.val(documentErrors.bucketListEmpty);
        }
        if (args.selected === null) {
          input.val(documentErrors.bucketNotExist);
          input.css('color', '#C00');
        }
      }
    });

    (function(){
      var latestSearch;
      var currentFilterParams;

      Cell.subscribeMultipleValues(function (term, filterParams) {
        latestSearch = term;
        currentFilterParams = filterParams;
      }, self.lookupIdCell, self.filter.filterParamsCell);

      docsLookup.keyup(function (e) {
        var docsLookupVal = $.trim($(this).val());
        if (latestSearch === docsLookupVal) {
          return true;
        }
        if (!docsLookupVal) {
          delete currentFilterParams.startkey;
          delete currentFilterParams.endkey;
          self.lookupIdCell.setValue(undefined);
        } else {
          self.lookupIdCell.setValue(docsLookupVal);
          var start = JSON.stringify(docsLookupVal);
          var end = JSON.stringify(docsLookupVal + String.fromCharCode(0xffff));
          if (currentFilterParams.descending && currentFilterParams.descending === 'true') {  
            currentFilterParams.startkey = end;
            currentFilterParams.endkey = start;
          } else {
            currentFilterParams.startkey = start;
            currentFilterParams.endkey = end;
          }
        }
        self.filter.rawFilterParamsCell.setValue($.isEmptyObject(currentFilterParams) ? undefined : $.param(currentFilterParams, true));
        self.documentsPageNumberCell.setValue(0);
      });
    })();

    //CRUD
    (function () {
      var modal;
      var spinner;
      var dbURL;
      var currentDocUrl;
      var currentDocId;

      self.dbURLCell.subscribeValue(function (url) {;
        dbURL = url;
      });

      self.currentDocURLCell.subscribeValue(function (url) {
        currentDocUrl = url;
      });

      self.currentDocumentIdCell.subscribeValue(function (id) {
        currentDocId = id;
      });

      function startSpinner(dialog) {
        modal = new ModalAction();
        spinner = overlayWithSpinner(dialog);
      }

      function stopSpinner() {
        modal.finish();
        spinner.remove();
      }

      function checkOnExistence(checkId, checkDoc) {
        var newUrl = buildDocURL(dbURL, checkId);

        couchReq("GET", newUrl, null,
          function (doc) {
            stopSpinner();
            if (doc) {
              createDocWarning.text(documentErrors.alreadyExists).show();
            }
          },
          function (error) {
            couchReq("PUT", newUrl, checkDoc, function () {
              stopSpinner();
              hideDialog(createDocDialog);
              self.documentIdCell.setValue(checkId);
            }, function (error, num, unexpected) {
              stopSpinner();
              if (error.reason) {
                createDocWarning.text(error.reason).show();
              } else {
                unexpected();
              }
              self.currentPageDocsURLCell.recalculate();
            });
          }
        );
      }

      docCreateBtn.click(function (e) {
        e.preventDefault();
        createDocWarning.text('');

        showDialog(createDocDialog, {
          eventBindings: [['.save_button', 'click', function (e) {
            e.preventDefault();
            var val = $.trim(createDocInput.val());
            if (val) {
              startSpinner(createDocDialog);
              var preDefinedDoc = {"click":"to edit", 
                "new in 2.0":"there are no reserved field names"};
              checkOnExistence(val, preDefinedDoc);
            } else {
              createDocWarning.text(documentErrors.idEmpty).show();
            }
          }]]
        });
      });

      docSaveAsBtn.click(function (e) {
        e.preventDefault();
        createDocWarning.text('');

        showDialog(createDocDialog, {
          eventBindings: [['.save_button', 'click', function (e) {
            e.preventDefault();
            var json = onDocValueUpdate(self.jsonCodeEditor.getValue());
            if (json) {
              var val = $.trim(createDocInput.val());
              if (val) {
                startSpinner(createDocDialog);
                checkOnExistence(val, json);
              } else {
                createDocWarning.text(documentErrors.idEmpty).show();
              }
            } else {
              hideDialog(createDocDialog);
            }
          }]]
        });
      });

      docSaveBtn.click(function (e) {
        e.preventDefault();
        editingNotice.text('');
        var json = onDocValueUpdate(self.jsonCodeEditor.getValue());
        if (json) {
          enableSaveBtn(false);
          showDocumentPendingState(true);
          couchReq('PUT', currentDocUrl, json, function () {
            couchReq("GET", currentDocUrl, undefined, function (doc) {
              showDocumentPendingState(false);
              tryShowJson(doc);
            });
          }, function (error, num, unexpected) {
            showDocumentPendingState(false);
            if (error.reason) {
              editingNotice.text(error.reason).show();
            } else {
              unexpected();
            }
          });
        }
      });

      self.deleteDocument = function (id, success) {
        deleteDocDialogWarning.text('').hide();
        showDialog(deleteDocDialog, {
          eventBindings: [['.save_button', 'click', function (e) {
            e.preventDefault();
            startSpinner(deleteDocDialog);
            couchReq('DELETE', buildDocURL(dbURL, id), null, function () {
              stopSpinner();
              hideDialog(deleteDocDialog);
              self.currentPageDocsCell.recalculate();
              if (success) {
                success();
              }
            }, function (error, num, unexpected) {
              stopSpinner();
              if (error.reason) {
                deleteDocDialogWarning.text(error.reason).show();
              } else {
                unexpected();
              }
            });
          }]]
        });
      }

      docDeleteBtn.click(function (e) {
        e.preventDefault();
        self.deleteDocument(currentDocId, function () {
          self.documentIdCell.setValue(undefined);
        });
      });
    })();
  },
  onLeave: function () {
    var self = this;
    self.documentsPageNumberCell.setValue(undefined);
    self.lookupIdCell.setValue(undefined);
  },
  onEnter: function () {
  },
  navClick: function () {
    this.onLeave();
    this.onEnter();
  }
};
