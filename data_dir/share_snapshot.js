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
			l = document.getElementsByClassName("fa-share-square-o");
			var ev = document.createEvent("MouseEvents");
        	ev.initEvent("click", true, true);
        	l[0].parentElement.dispatchEvent(ev);
		});
    }, 15000);
	window.setTimeout(function() {
		page.evaluate(function() {
			l = document.getElementsByClassName("icon-gf-snapshot");
			var ev = document.createEvent("MouseEvents");
        	ev.initEvent("click", true, true);
        	l[0].parentElement.dispatchEvent(ev);
		});
	}, 20000);
	window.setTimeout(function() {
		page.evaluate(function() {
			l = document.getElementsByClassName("fa-cloud-upload");
			var ev = document.createEvent("MouseEvents");
        	ev.initEvent("click", true, true);
        	l[1].dispatchEvent(ev);
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
