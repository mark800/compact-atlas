!function(r,e){"object"==typeof exports&&"undefined"!=typeof module?e(exports):"function"==typeof define&&define.amd?define(["exports"],e):e((r=r||self).LosslessJSON={})}(this,function(r){"use strict";var e=!0;function i(r){return r&&void 0!==r.circularRefs&&null!==r.circularRefs&&(e=!0===r.circularRefs),{circularRefs:e}}function o(r){return(o="function"==typeof Symbol&&"symbol"==typeof Symbol.iterator?function(r){return typeof r}:function(r){return r&&"function"==typeof Symbol&&r.constructor===Symbol&&r!==Symbol.prototype?"symbol":typeof r})(r)}function f(r,e){for(var n=0;n<e.length;n++){var t=e[n];t.enumerable=t.enumerable||!1,t.configurable=!0,"value"in t&&(t.writable=!0),Object.defineProperty(r,t.key,t)}}var u=function(){function e(r){!function(r,e){if(!(r instanceof e))throw new TypeError("Cannot call a class as a function")}(this,e),this.value=function r(e){{if("string"==typeof e){if(!/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(e))throw new Error('Invalid number (value: "'+e+'")');return e}if("number"!=typeof e)return r(e&&e.valueOf());if(15<a(e+"").length)throw new Error("Invalid number: contains more than 15 digits (value: "+e+")");if(isNaN(e))throw new Error("Invalid number: NaN");if(!isFinite(e))throw new Error("Invalid number: Infinity");return e+""}}(r),this.type="LosslessNumber",this.isLosslessNumber=!0}var r,n,t;return r=e,(n=[{key:"valueOf",value:function(){var r=parseFloat(this.value),e=a(this.value);if(15<e.length)throw new Error("Cannot convert to number: number would be truncated (value: "+this.value+")");if(!isFinite(r))throw new Error("Cannot convert to number: number would overflow (value: "+this.value+")");if(Math.abs(r)<Number.MIN_VALUE&&!/^0*$/.test(e))throw new Error("Cannot convert to number: number would underflow (value: "+this.value+")");return r}},{key:"toString",value:function(){return this.value}}])&&f(r.prototype,n),t&&f(r,t),e}();function a(r){return("string"!=typeof r?r+"":r).replace(/^-/,"").replace(/e.*$/,"").replace(/^0\.?0*|\./,"")}function l(r,e,n,t){return Array.isArray(n)?t.call(r,e,function(r,e){for(var n=[],t=0;t<r.length;t++)n[t]=l(r,t+"",r[t],e);return n}(n,t)):n&&"object"===o(n)&&!n.isLosslessNumber?t.call(r,e,function(r,e){var n={};for(var t in r)r.hasOwnProperty(t)&&(n[t]=l(r,t,r[t],e));return n}(n,t)):t.call(r,e,n)}function n(r){return encodeURIComponent(r.replace(/\//g,"~1").replace(/~/g,"~0"))}function c(r){return decodeURIComponent(r).replace(/~1/g,"/").replace(/~0/g,"~")}function s(r){return"#/"+r.map(n).join("/")}var v={NULL:0,DELIMITER:1,NUMBER:2,STRING:3,SYMBOL:4,UNKNOWN:5},t={"":!0,"{":!0,"}":!0,"[":!0,"]":!0,":":!0,",":!0},h={'"':'"',"\\":"\\","/":"/",b:"\b",f:"\f",n:"\n",r:"\r",t:"\t"},p="",d=0,y="",b="",g=v.NULL,m=[],w=[];function I(){d++,y=p.charAt(d)}function E(){for(g=v.NULL,b="";" "===y||"\t"===y||"\n"===y||"\r"===y;)I();if(t[y])return g=v.DELIMITER,b=y,void I();if(L(y)||"-"===y){if(g=v.NUMBER,"-"===y){if(b+=y,I(),!L(y))throw R("Invalid number, digit expected",d)}else"0"===y&&(b+=y,I());for(;L(y);)b+=y,I();if("."===y){if(b+=y,I(),!L(y))throw R("Invalid number, digit expected",d);for(;L(y);)b+=y,I()}if("e"===y||"E"===y){if(b+=y,I(),"+"!==y&&"-"!==y||(b+=y,I()),!L(y))throw R("Invalid number, digit expected",d);for(;L(y);)b+=y,I()}}else if('"'!==y){if(!N(y)){for(g=v.UNKNOWN;""!==y;)b+=y,I();throw R('Syntax error in part "'+b+'"')}for(g=v.SYMBOL;N(y);)b+=y,I()}else{for(g=v.STRING,I();""!==y&&'"'!==y;)if("\\"===y){I();var r=h[y];if(void 0!==r)b+=r,I();else{if("u"!==y)throw R('Invalid escape character "\\'+y+'"',d);I();for(var e="",n=0;n<4;n++){if(!/^[0-9a-fA-F]/.test(y))throw R("Invalid unicode character");e+=y,I()}b+=String.fromCharCode(parseInt(e,16))}}else b+=y,I();if('"'!==y)throw R("End of string expected");I()}}function N(r){return/^[a-zA-Z_]/.test(r)}function L(r){return"0"<=r&&r<="9"}function R(r,e){void 0===e&&(e=d-b.length);var n=new SyntaxError(r+" (char "+e+")");return n.char=e,n}function S(){if(g!==v.DELIMITER||"{"!==b)return function(){if(g!==v.DELIMITER||"["!==b)return function(){if(g!==v.STRING)return function(){if(g!==v.NUMBER)return function(){if(g!==v.SYMBOL)return function(){throw R(""===b?"Unexpected end of json string":"Value expected")}();if("true"===b)return E(),!0;if("false"===b)return E(),!1;if("null"!==b)throw R('Unknown symbol "'+b+'"');return E(),null}();var r=new u(b);return E(),r}();var r=b;return E(),r}();E();var r=[];if(g===v.DELIMITER&&"]"===b)return E(),r;var e=w.length;w[e]=r;for(;m[e]=r.length+"",r.push(S()),g===v.DELIMITER&&","===b;)E();if(g===v.DELIMITER&&"]"===b)return E(),w.length=e,m.length=e,r;throw R('Comma or end of array "]" expected')}();var r,e;E();var n={};if(g===v.DELIMITER&&"}"===b)return E(),n;var t=w.length;for(w[t]=n;;){if(g!==v.STRING)throw R("Object key expected");if(e=b,E(),g!==v.DELIMITER||":"!==b)throw R("Colon expected");if(E(),n[m[t]=e]=S(),g!==v.DELIMITER||","!==b)break;E()}if(g!==v.DELIMITER||"}"!==b)throw R('Comma or end of object "}" expected');return E(),"string"==typeof(r=n).$ref&&1===Object.keys(r).length?function(r){if(!i().circularRefs)return r;for(var e=function(r){var e=r.split("/").map(c);if("#"!==e.shift())throw SyntaxError("Cannot parse JSON Pointer: no valid URI fragment");return""===e[e.length-1]&&e.pop(),e}(r.$ref),n=0;n<e.length;n++)if(e[n]!==m[n])throw new Error('Invalid circular reference "'+r.$ref+'"');return w[e.length]}(n):(w.length=t,m.length=t,n)}var M=[],O=[];function x(r,e,n){O=[],M=[];var t,o="function"==typeof e?e.call({"":r},"",r):r;return"number"==typeof n?10<n?t=A(" ",10):1<=n&&(t=A(" ",n)):"string"==typeof n&&""!==n&&(t=n),T(o,e,t,"")}function T(r,e,n,t){return"boolean"==typeof r||r instanceof Boolean||null===r||"number"==typeof r||r instanceof Number||"string"==typeof r||r instanceof String||r instanceof Date?JSON.stringify(r):r&&r.isLosslessNumber?r.value:Array.isArray(r)?function(r,e,n,t){var o=n?t+n:void 0,i=n?"[\n":"[";if(C(r))return D(r,e,n,t);var f=O.length;O[f]=r;for(var u=0;u<r.length;u++){var a=u+"",l="function"==typeof e?e.call(r,a,r[u]):r[u];n&&(i+=o),void 0!==l&&"function"!=typeof l?(M[f]=a,i+=T(l,e,n,o)):i+="null",u<r.length-1&&(i+=n?",\n":",")}return O.length=f,M.length=f,i+=n?"\n"+t+"]":"]"}(r,e,n,t):r&&"object"===o(r)?U(r,e,n,t):void 0}function U(r,e,n,t){var o=n?t+n:void 0,i=!0,f=n?"{\n":"{";if("function"==typeof r.toJSON)return x(r.toJSON(),e,n);if(C(r))return D(r,e,n,t);var u,a,l,c=O.length;for(var s in O[c]=r)if(r.hasOwnProperty(s)){var v="function"==typeof e?e.call(r,s,r[s]):r[s];u=s,l=e,void 0===(a=v)||"function"==typeof a||Array.isArray(l)&&!function(r,e){for(var n=0;n<r.length;n++)if(r[n]==e)return!0;return!1}(l,u)||(i?i=!1:f+=n?",\n":",",f+=n?o+'"'+s+'": ':'"'+s+'":',M[c]=s,f+=T(v,e,n,o))}return O.length=c,M.length=c,f+=n?"\n"+t+"}":"}"}function C(r){return-1!==O.indexOf(r)}function D(r,e,n,t){if(!i().circularRefs)throw new Error('Circular reference at "'+s(M)+'"');var o=O.indexOf(r);return U({$ref:s(M.slice(0,o))},e,n,t)}function A(r,e){for(var n="";0<e--;)n+=r;return n}r.LosslessNumber=u,r.config=i,r.parse=function(r,e){d=0,y=(p=r).charAt(0),b="",g=v.NULL,w=[],m=[],E();var n,t=S();if(""!==b)throw R("Unexpected characters");return e?l({"":n=t},"",n,e):t},r.stringify=x,Object.defineProperty(r,"__esModule",{value:!0})});
//# sourceMappingURL=lossless-json.js.map