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
;(function($){
  $.fn.observePotentialChanges = (function () {
    var intervalId;
    var period = 20;
    var maxIdlePeriods = 4;

    var hadActivity;
    var idlePeriods;

    var idGen = 0;
    var callbacks = {};
    var callbacksSize = 0;

    function timerCallback() {
      for (var i in callbacks) {
        (callbacks[i])();
      }

      if (!hadActivity) {
        if (++idlePeriods >= maxIdlePeriods) {
          suspendTimer();
          idlePeriods = 0;
        }
      } else {
        idlePeriods = 0;
        hadActivity = undefined;
      }
    }
    function activateTimer() {
      hadActivity = true;
      if (intervalId != null)
        return;
      intervalId = setInterval(timerCallback, period);
    }
    function suspendTimer() {
      if (intervalId == null)
        return;
      clearInterval(intervalId);
      intervalId = null;
    }
    function requestTimer(callback) {
      callbacks[++idGen] = callback;
      callbacksSize++;

      activateTimer();
      return idGen;
    }
    function releaseTimer(id) {
      delete callbacks[id];
      if (--callbacksSize == 0)
        suspendTimer();
    }

    return function (callback) {
      var query = this;
      var events = 'change mousemove click dblclick keyup keydown';
      var boundF;
      var id;

      var instance = {
        stopObserving: function () {
          releaseTimer(id);
          unbindEvents();
        },
        callback: callback
      }

      function cb() {
        callback.call(null, instance);
        if (!boundF)
          bindEvents();
      }

      id = requestTimer(cb);

      function bindEvents() {
        query.bind(events, boundF = function (e) {
          activateTimer();
          unbindEvents();
        });
      }

      function unbindEvents() {
        query.unbind(events, boundF);
        boundF = null;
      }

      bindEvents();
      return instance;
    }
  })();

  $.fn.observeInput = function (callback) {
    var query = this;
    var prevObject = query.prevObject || $('html > body');
    var lastValue = query.val();

    return prevObject.observePotentialChanges(function (instance) {
      var newValue = query.val();
      if (newValue == lastValue)
        return;

      lastValue = newValue;
      callback.call(query, lastValue, instance);
    });
  };

  $.fn.boolAttr = function(attr, value) {
    var $this = $(this);
    if (value) {
      $this.attr(attr, attr);
    } else {
      $this.removeAttr(attr);
    }
    return $this;
  };

  $.fn.bindListCell = function (cell, options) {
    var q = this;
    q.need(1);
    if (!q.is('select')) {
      throw new Error('only select is supported');
    }
    if (q.data('listCellBinding')) {
      throw new Error('already bound');
    }
    var onChange = options.onChange;
    if (!onChange) {
      throw new Error('I need onchange');
    }

    var latestOnChangeVal;
    var latestOptionsList;

    var applyWidget = options.applyWidget || function (q) {
      q.combobox();
    };

    var unapplyWidget = options.unapplyWidget || function (q) {
      q.combobox('destroy');
    };

    var buildOptions = options.buildOptions || function (q, selected, list) {
      _.each(list, function (pair) {
        var option = $("<option value='" + escapeHTML(pair[0]) + "'>" + escapeHTML(pair[1]) + "</option>");
        option.boolAttr('selected', selected === pair[0]);
        q.append(option);
      });
    };

    onChange = (function (onChange) {
      return function (e) {
        return onChange.call(this, e, (latestOnChangeVal = $(this).val()));
      }
    })(onChange);

    var subscription = cell.subscribeValue(function (args) {
      if (args && _.isEqual(latestOnChangeVal, args.list) && (args.selected || '') === latestOnChangeVal) {
        // don't do anything if list of options and selected option is same
        return;
      }
      q.unbind('change', onChange);
      unapplyWidget(q);
      q.empty();
      if (!args) {
        return;
      }
      var selected = args.selected || '';
      latestOnChangeVal = undefined;
      buildOptions(q, selected, args.list);
      applyWidget(q.bind('change', onChange));
      var input = q.next('input');
      input.val(q.children(':selected').text());
      if (options.onRenderDone && typeof options.onRenderDone === 'function') {
        options.onRenderDone(input, args);
      }
    });

    q.data('listCellBinding', {
      destroy: function () {
        subscription.cancel();
        unapplyWidget(q);
        q.unbind('change', onChange);
      }
    });
  };

  $.fn.unbindListCell = function () {
    var binding = this.data('listCellBinding');
    if (binding) {
      binding.destroy();
    }
    this.removeData('listCellBinding');
  };

  $.fn.need = function (howmany) {
    var $this = $(this);
    if ($this.length != howmany) {
      console.log("Expected jquery of length ", howmany, ", got: ", $this);
      throw new Error("Expected jquery of length " + howmany + ", got " + $this.length);
    }
    return $this;
  };

  /**
   * Combobox widget
   * @link http://jqueryui.com/demos/autocomplete/#combobox
   */
  $.widget( "ui.combobox", {
    _create: function() {
      var self = this,
          select = this.element.hide(),
          selected = select.children( ":selected" ),
          value = selected.val() ? selected.text() : "";
      var input = this.input = $( "<input>" )
        .insertAfter( select )
        .val( value )
        .autocomplete({
          delay: 0,
          minLength: 0,
          source: function( request, response ) {
            var matcher = new RegExp( $.ui.autocomplete.escapeRegex(request.term), "i" );
            response( select.children( "option" ).map(function() {
              var text = $( this ).text();
              if ( ( !request.term || matcher.test(text) ) )
                return {
                  label: text.replace(
                    new RegExp(
                      "(?![^&;]+;)(?!<[^<>]*)(" +
                      $.ui.autocomplete.escapeRegex(request.term) +
                      ")(?![^<>]*>)(?![^&;]+;)", "gi"
                    ), "<strong>$1</strong>" ),
                  value: text,
                  option: this
                };
            }) );
          },
          select: function( event, ui ) {
            ui.item.option.selected = true;
            self._trigger( "selected", event, {
              item: ui.item.option
            });
            select.trigger('change');
          },
          change: function( event, ui ) {
            if ( !ui.item ) {
              var matcher = new RegExp( "^" + $.ui.autocomplete.escapeRegex( $(this).val() ) + "$", "i" ),
                  valid = false;
              select.children( "option" ).each(function() {
                if ( $( this ).text().match( matcher ) ) {
                  this.selected = valid = true;
                  return false;
                }
              });
              if ( !valid ) {
                // remove invalid value, as it didn't match anything
                $( this ).val( "" );
                select.val( "" );
                input.data( "autocomplete" ).term = "";
                $(this).trigger('focus');
                self.button.trigger('click');
                return false;
              }
            }
          }
        })
        .addClass( "ui-widget ui-widget-content ui-corner-left" );

      input.click(function() {
          $(this).select();
        })
        .keydown(function(ev) {
          var visible = $(this).autocomplete('widget').find('li:visible');
          // catch tab and enter keys
          if (ev.which == 9 || ev.which == 13) {
            if (visible.length == 1) {
              $(this).val($(visible[0]).text())
                .prev('select').find('option:contains('+$(visible[0]).text()+')')
                .attr('selected', 'selected').trigger('change');
            } else if (visible.length > 1) {
              ev.preventDefault();
            }
            if (ev.which == 13) {
              $(this).blur();
            }
          }
        })
        .data( "autocomplete" )._renderItem = function( ul, item ) {
          return $( "<li></li>" )
            .data( "item.autocomplete", item )
            .append( "<a>" + item.label + "</a>" )
            .appendTo( ul );
        };

      this.button = $( "<button type='button'>&nbsp;</button>" )
        .attr( "tabIndex", -1 )
        .attr( "title", "Show All Items" )
        .insertAfter( input )
        .button({
          icons: {
            primary: "ui-icon-triangle-1-s"
          },
          text: false
        })
        .removeClass( "ui-corner-all" )
        .addClass( "ui-corner-right ui-button-icon" )
        .click(function() {
          // close if already visible
          if ( input.autocomplete( "widget" ).is( ":visible" ) ) {
            input.autocomplete( "close" );
            return;
          }

          // pass empty string as value to search for, displaying all results
          input.autocomplete( "search", "" );
          input.focus();
        });
    },

    destroy: function() {
      this.input.remove();
      this.button.remove();
      this.element.show();
      $.Widget.prototype.destroy.call( this );
    }
  });

  $('.block-expander').live('click', function () {
    $(this).closest('.darker_block').toggleClass('closed');
  });
  $('.block-expander .buttons a').live('click', function(ev) {
    ev.stopPropagation();
  });
})(jQuery);
