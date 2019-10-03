"use strict";
var page = require("webpage").create();
var system = require('system');

page.onConsoleMessage = function(msg) {
  console.log(msg);
}
page.viewportSize = { width: 1920, height: 1080 };

var address = system.args[1];

page.open(address, function(status) {
    if (status !== "success") {
        console.log("Error to load page " + address);
        phantom.exit();
    }
    window.setTimeout(function () {
        page.evaluate(function() {
            var ev = document.createEvent('MouseEvent');
            ev.initMouseEvent('click', true, true);
            var l = document.getElementsByClassName("navbar-button--share");
            l[0].dispatchEvent(ev)
        });
    }, 15000);
    window.setTimeout(function() {
        page.evaluate(function() {
            var ev = document.createEvent('MouseEvent');
            ev.initMouseEvent('click', true, true);
            l = document.getElementsByClassName("gf-tabs-item");
            l[1].firstElementChild.dispatchEvent(ev);
        });
    }, 20000);
    window.setTimeout(function() {
        page.evaluate(function() {
            var ev = document.createEvent('MouseEvent');
            ev.initMouseEvent('click', true, true);
            l = document.getElementsByClassName("gf-form-button-row");
            l[0].children[1].dispatchEvent(ev);
        });
    }, 25000);
    window.setTimeout(function() {
        page.evaluate(function() {
            l = document.getElementsByClassName("fa-external-link-square");
            console.log(l[0].parentElement.lastChild.nodeValue.trim());
        });
        phantom.exit();
    }, 60000);
});
