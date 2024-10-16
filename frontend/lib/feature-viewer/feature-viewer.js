var FeatureViewer = (function () {
    function FeatureViewer(id, sequence, el, options) {
        // d3.selection.prototype.duration = function () { return this }
        // d3.selection.prototype.transition = function () { return this }

        var labelSizeLookup = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,5,7,6,9,8,3,3,3,6,6,3,4,3,4,6,6,6,6,6,6,6,6,6,6,3,3,6,6,6,5,9,7,7,7,8,6,6,8,8,3,3,7,6,10,8,8,7,8,7,6,6,8,6,10,6,6,6,4,4,4,6,5,6,6,7,5,7,6,4,6,7,3,3,6,3,10,7,7,7,7,5,5,4,7,6,8,6,6,5,4,6,4,6,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,6,6,6,6,6,6,6,9,4,5,6,0,9,5,5,6,4,4,6,7,7,3,3,4,4,5,8,8,8,5,7,7,7,7,7,7,9,7,6,6,6,6,3,3,3,3,8,8,8,8,8,8,8,6,8,8,8,8,8,6,7,7,6,6,6,6,6,6,9,5,6,6,6,6,3,3,3,3,6,7,7,7,7,7,7,6,7,7,7,7,7,6,7,6];

        var svgElement = null;
        var svgContainer = null;

        var intLength = Number.isInteger(sequence) ? sequence : null;
        var fvLength = intLength | sequence.length;
        var features = [];
        var SVGOptions = {
            showSequence: false,
            brushActive: false,
            verticalLine: false,
            dottedSequence: true
        };

        var offset = { start: 1, end: fvLength };
        if (options && options.offset) {
            offset = options.offset;
            if (offset.start < 1) {
                offset.start = 1;
                console.warn("WARNING ! offset.start should be > 0. Thus, it has been reset to 1.");
            }
        }

        var onZoom = options.onZoom || null;
        var onHeightChanged = options.onHeightChanged || null;

        var pathLevel = 0;
        var svg;
        var yData = [];
        var yAxisSVG;
        var yAxisSVGgroup;
        var Yposition = 20;
        var level = 0;
        var seqShift = 0;
        var zoomMax = 50;
        var current_extend = {
            length: offset.end - offset.start,
            start: offset.start,
            end: offset.end
        }
        var featureSelected = {};
        var animation = true;
        var sbcRip = null;

        const transitionDuration = 66;

        var margin = {
            top: 20,
            right: 20,
            bottom: 20,
            left: 110
        };

        var width, height, scaling, scalingPosition;
        var lineBond, lineGen, lineYscale, line;
        var xAxis1, xAxis2, yAxisScale, yAxis, brush;

        function colorSelectedFeat(feat, object) {
            //change color && memorize
            if (featureSelected !== {}) d3.select(featureSelected.id).style("fill", featureSelected.originalColor);
            if (object.type !== "path" && object.type !== "line") {
                featureSelected = { "id": feat, "originalColor": d3.select(feat).style("fill") || object.color };
                d3.select(feat).style("fill", "orangered");
            }
        }

        function X(d) {
            return scaling(d.x);
        }

        function displaySequence(seq) {
            return width / seq > 5;
        }

        function rectWidth(d) {
            return (scaling(d.y) - scaling(d.x));
        }

        function rectX(object) {
            if (object.x === object.y) {
                return scaling(object.x - 0.4);
            }
            return scaling(object.x);
        }

        function rectWidth2(d) {
            if (d.x === d.y) {
                if (scaling(d.x + 0.4) - scaling(d.x - 0.4) < 2) return 2;
                else return scaling(d.x + 0.4) - scaling(d.x - 0.4);
            }
            return (scaling(d.y) - scaling(d.x));
        };

        function rectPoints(object, width) {
            return function(d) {
                var rectHeight = object.height;
                var rectShift = rectHeight + rectHeight / 3;
                var y = d.level * rectShift;
                var h = rectHeight;
                var w = width || rectWidth(d);
                var b = y + h;
                if (typeof (d.reverse) != "boolean") {
                    return "0," + y + " 0," + b + " " + w + "," + b + " " + w + "," + y;
                }
                var m = y + (h / 2);
                if (d.reverse == true) {
                    return "5," + y + " 0," + m + " 5," + b + " " + w + "," + b + " " + (w - 5) + "," + m + " " + w + "," + y;
                } else {
                    return "0," + y + " 5," + m + " 0," + b + " " + (w - 5) + "," + b + " " + (w) + "," + m + " " + (w - 5) + "," + y;
                }
            }
        }

        var uniqueWidth = function (d) {
            return (scaling(1));
        };

        function descriptionElipsis(d, fitWidth) {
            var pxPerLetter = d.descriptionWidth / d.description.length;
            var charsToDisplay = 1;
            while (pxPerLetter * (charsToDisplay + 3) < fitWidth) {
                charsToDisplay++;
            }
            if ((charsToDisplay + 3) >= d.description.length) {
                return d.description;
            }
            return d.description.substring(0, charsToDisplay) + "…";
        }

        function addLevel(array) {
            var leveling = [];
            array.forEach(function (d) {
                if (leveling === []) {
                    leveling.push(d.y);
                    d.level = 0;
                } else {
                    var placed = false;
                    for (var k = leveling.length; k >= 0; --k) {
                        if (d.x > leveling[k]) {
                            placed = true;
                            d.level = k;
                            leveling[k] = d.y;
                            break;
                        }
                    }
                    if (placed === false) {
                        leveling.push(d.y);
                        d.level = leveling.length - 1;
                    }
                }
            });
            return leveling.length;
        }

        function addLevelToBond(array) {
            var leveling = [];
            var newArray = [];
            array.forEach(function (d) {
                if (leveling === []) {
                    leveling.push(d[2].x);
                    d[1].y = 1;
                } else {
                    var placed = false;
                    for (var k = 0; k < leveling.length; k++) {
                        if (d[0].x > leveling[k]) {
                            placed = true;
                            d[1].y = k + 1;
                            leveling[k] = d[2].x;
                            break;
                        }
                    }
                    if (placed === false) {
                        leveling.push(d[2].x);
                        d[1].y = leveling.length;
                    }
                }
            });
            return leveling.length;
        }

        function shadeBlendConvert(p, from, to) {
            if (typeof (p) != "number" || p < -1 || p > 1 || typeof (from) != "string" || (from[0] != 'r' && from[0] != '#') || (typeof (to) != "string" && typeof (to) != "undefined")) return null; //ErrorCheck
            if (!this.sbcRip) this.sbcRip = function (d) {
                var l = d.length, RGB = new Object();
                if (l > 9) {
                    d = d.split(",");
                    if (d.length < 3 || d.length > 4) return null;//ErrorCheck
                    RGB[0] = i(d[0].slice(4)), RGB[1] = i(d[1]), RGB[2] = i(d[2]), RGB[3] = d[3] ? parseFloat(d[3]) : -1;
                } else {
                    if (l == 8 || l == 6 || l < 4) return null; //ErrorCheck
                    if (l < 6) d = "#" + d[1] + d[1] + d[2] + d[2] + d[3] + d[3] + (l > 4 ? d[4] + "" + d[4] : ""); //3 digit
                    d = i(d.slice(1), 16), RGB[0] = d >> 16 & 255, RGB[1] = d >> 8 & 255, RGB[2] = d & 255, RGB[3] = l == 9 || l == 5 ? r(((d >> 24 & 255) / 255) * 10000) / 10000 : -1;
                }
                return RGB;
            }
            var i = parseInt, r = Math.round, h = from.length > 9, h = typeof (to) == "string" ? to.length > 9 ? true : to == "c" ? !h : false : h, b = p < 0, p = b ? p * -1 : p, to = to && to != "c" ? to : b ? "#000000" : "#FFFFFF", f = sbcRip(from), t = sbcRip(to);
            if (!f || !t) return null; //ErrorCheck
            if (h) return "rgb(" + r((t[0] - f[0]) * p + f[0]) + "," + r((t[1] - f[1]) * p + f[1]) + "," + r((t[2] - f[2]) * p + f[2]) + (f[3] < 0 && t[3] < 0 ? ")" : "," + (f[3] > -1 && t[3] > -1 ? r(((t[3] - f[3]) * p + f[3]) * 10000) / 10000 : t[3] < 0 ? f[3] : t[3]) + ")");
            else return "#" + (0x100000000 + (f[3] > -1 && t[3] > -1 ? r(((t[3] - f[3]) * p + f[3]) * 255) : t[3] > -1 ? r(t[3] * 255) : f[3] > -1 ? r(f[3] * 255) : 255) * 0x1000000 + r((t[0] - f[0]) * p + f[0]) * 0x10000 + r((t[1] - f[1]) * p + f[1]) * 0x100 + r((t[2] - f[2]) * p + f[2])).toString(16).slice(f[3] > -1 || t[3] > -1 ? 1 : 3);
        }

        function updateXaxis(position) {
            svgContainer.selectAll(".Xaxis")
                .attr("transform", "translate(0," + (position + 20) + ")")
        }

        function updateSVGHeight(position) {
            svg.attr("height", position + 60 + "px");
            svg.select("#clip rect").attr("height", position + 60 + "px");

            if (typeof(onHeightChanged) == "function") {
                onHeightChanged(position + 60);
            }
        }

        function addYAxis() {
            yAxisSVG = svg.append("g")
                .attr("class", "pro axis")
                .attr("transform", "translate(0," + margin.top + ")");
            updateYaxis();
        }

        function updateYaxis() {
            function expandArrow() {
                var e = d3.select(this);
                e.select("text").attr("clip-path", null);
                e.select("rect").attr("width", width + 105);
                e.select(".label-fade").style("opacity", 0);
                e.select("polygon").style("display", "none");
            }

            function collapseArrow() {
                var e = d3.select(this);
                e.select("rect").attr("width", 90);
                e.select("text").attr("clip-path", "url(#arrow-clip)");
                e.select(".label-fade").style("opacity", 1);
                e.select("polygon").style("display", "block");
            }

            function toggleArrow() {
                var e = d3.select(this);
                if (e.select(".label-fade").style("opacity") == 0) {
                    (collapseArrow.bind(this))();
                } else {
                    (expandArrow.bind(this))();
                }
            }

            yAxisSVGgroup = yAxisSVG
                .selectAll(".yaxis")
                .data(yData)
                .enter()
                .append("g")
                .on('mouseenter', expandArrow)
                .on('mouseleave', collapseArrow)
                .on('touchstart', toggleArrow)
                .style("display", function (d) {
                    if (d.title == "") return "none";
                })

            yAxisSVGgroup
                .append("rect")
                .attr("class", function (d) {
                    if (d.filter) return d.filter.split(" ").join("_") + "ArrowBody";
                    return "ArrowBody";
                })
                .style("stroke", "") // colour the line
                .style("fill", "#DFD5D3") // remove any fill colour
                .attr("x", function() { return margin.left - 105; })
                .attr("y", function(d) { return d.y - 3 })
                .attr("width", function (d) { return 90 })
                .attr("height", 15);

            yAxisSVGgroup
                .append("text")
                .attr("class", "yaxis")
                .attr("text-anchor", "start")
                .attr("transform", function (d) { return "translate(" + (margin.left - 102) + "," + (d.y + 8) + ")"})     
                .text(function (d) {
                    return d.title
                })
                .attr("clip-path", "url(#arrow-clip)");
                
            yAxisSVGgroup
                .append("rect") // attach a polygon
                .attr('class', 'label-fade')
                .style("stroke", "")
                .style("fill", "url(#label-fade-out)") // remove any fill colour
                .attr("x", function() { return margin.left - 105; })
                .attr("y", function(d) { return d.y - 3 })
                .attr("width", function (d) { return 90 })
                .attr("height", 15);


            yAxisSVGgroup
                .append("polygon") // attach a polygon
                .attr("class", function (d) {
                    if (d.filter) return d.filter.split(" ").join("_") + "Arrow label-fade";
                    return "Arrow label-fade";
                })
                .style("stroke", "") // colour the line
                .style("fill", "#DFD5D3") // remove any fill colour
                .attr("points", function (d) {
                    return (margin.left - 15) + "," + (d.y + 12) + ", " + (margin.left - 7) + "," + (d.y + 4.5) + ", " + (margin.left - 15) + "," + (d.y - 3); // x,y points
                });
                
        }
        function forcePropagation(item) {
            item.on('mousedown', function () {
                var brush_elm = svg.select(".brush").node();
                var new_click_event = new Event('mousedown');
                new_click_event.pageX = d3.event.pageX;
                new_click_event.clientX = d3.event.clientX;
                new_click_event.pageY = d3.event.pageY;
                new_click_event.clientY = d3.event.clientY;
                if (brush_elm) {
                    brush_elm.dispatchEvent(new_click_event);
                }
            });
        }

        var preComputing = {
            path: function (object) {
                if (typeof object.shouldSort === 'undefined' || object.shouldSort) {
                    object.data.sort(function (a, b) {
                        return a.x - b.x;
                    });
                }
                var level = addLevel(object.data);
                object.data = object.data.map(function (d) {
                    return [{
                        x: d.x,
                        y: 0,
                        id: d.id,
                        description: d.description,
                        color: d.color
                    }, {
                        x: d.y,
                        y: d.level + 1,
                        id: d.id
                    }, {
                        x: d.y,
                        y: 0,
                        id: d.id
                    }]
                })
                pathLevel = level * 10 + 5;
                object.height = level * 10 + 5;
            },
            line: function (object) {
                if (!object.height) object.height = 10;
                var shift = parseInt(object.height);
                var level = 0;
                for (var i in object.data) {
                    if (typeof object.shouldSort === 'undefined' || object.shouldSort) {
                        object.data[i].sort(function (a, b) {
                            return a.x - b.x;
                        });
                    }
                    if (object.data[i][0].y !== 0) {
                        object.data[i].unshift({
                            x: object.data[i][0].x - 1,
                            y: 0
                        })
                    }
                    if (object.data[i][object.data[i].length - 1].y !== 0) {
                        object.data[i].push({
                            x: object.data[i][object.data[i].length - 1].x + 1,
                            y: 0
                        })
                    }
                    var maxValue = Math.max.apply(Math, object.data[i].map(function (o) { return Math.abs(o.y); }));
                    level = maxValue > level ? maxValue : level;


                    object.data[i] = [object.data[i].map(function (d) {
                        return {
                            x: d.x,
                            y: d.y,
                            id: d.id,
                            description: d.description
                        }
                    })]
                }
                lineYscale.range([0, -(shift)]).domain([0, -(level)]);
                pathLevel = shift * 10 + 5;
                object.level = level;
                object.shift = shift * 10 + 5;
            },
            multipleRect: function (object) {
                if (typeof object.shouldSort === 'undefined' || object.shouldSort) {
                    object.data.sort(function (a, b) {
                        return a.x - b.x;
                    });
                }
                object.data = object.data.map(function (d) {
                    var descriptionWidth = 0;
                    for (var i = 0; i < d.description.length; ++i) {
                        var code = d.description.charCodeAt(i);
                        if (code < 256) {
                            descriptionWidth += labelSizeLookup[code];
                        } else {
                            descriptionWidth += 5;
                        }
                    }
                    d.descriptionWidth = descriptionWidth;
                    return d;
                })
                level = addLevel(object.data);
                pathLevel = level * 10 + 5;
            }
        };

        var fillSVG = {
            typeIdentifier: function (object) {
                if (object.type === "rect") {
                    preComputing.multipleRect(object);
                    yData.push({
                        title: object.name,
                        y: Yposition,
                        filter: object.filter
                    });
                    fillSVG.rectangle(object, Yposition);
                } else if (object.type === "text") {
                    fillSVG.sequence(object.data, Yposition);
                    yData.push({
                        title: object.name,
                        y: Yposition,
                        filter: object.filter
                    });
                    scaling.range([5, width - 5]);
                } else if (object.type === "unique") {
                    fillSVG.unique(object, Yposition);
                    yData.push({
                        title: object.name,
                        y: Yposition,
                        filter: object.filter
                    });
                } else if (object.type === "multipleRect") {
                    preComputing.multipleRect(object);
                    fillSVG.multipleRect(object, Yposition, level);
                    yData.push({
                        title: object.name,
                        y: Yposition,
                        filter: object.filter
                    });
                    Yposition += (level - 1) * 10;
                } else if (object.type === "path") {
                    preComputing.path(object);
                    fillSVG.path(object, Yposition);
                    Yposition += pathLevel;
                    yData.push({
                        title: object.name,
                        y: Yposition - 10,
                        filter: object.filter
                    });
                } else if (object.type === "line") {
                    if (!(Array.isArray(object.data[0]))) object.data = [object.data];
                    if (!(Array.isArray(object.color))) object.color = [object.color];
                    var negativeNumbers = false;
                    object.data.forEach(function (d) {
                        if (d.filter(function (l) { return l.y < 0 }).length) negativeNumbers = true;
                    });
                    preComputing.line(object);
                    fillSVG.line(object, Yposition);
                    Yposition += pathLevel;
                    yData.push({
                        title: object.name,
                        y: Yposition - 10,
                        filter: object.filter
                    });
                    Yposition += negativeNumbers ? pathLevel - 5 : 0;
                }
            },
            sequence: function (seq, position, start) {
                //Create group of sequence
                if (!start) var start = 0;
                svgContainer.append("g")
                    .attr("class", "seqGroup")
                    .selectAll(".AA")
                    .data(seq)
                    .enter()
                    .append("text")
                    .attr("clip-path", "url(#clip)")
                    .attr("class", "AA")
                    .attr("text-anchor", "middle")
                    .attr("x", function (d, i) {
                        return scaling.range([5, width - 5])(i + start)
                    })
                    .attr("y", position)
                    .attr("font-size", "10px")
                    .attr("font-family", "monospace")
                    .text(function (d, i) {
                        return d
                    });
            },
            sequenceLine: function () {
                //Create line to represent the sequence
                if (SVGOptions.dottedSequence) {
                    var dottedSeqLine = svgContainer.selectAll(".sequenceLine")
                        .data([[{ x: 1, y: 12 }, { x: fvLength, y: 12 }]])
                        .enter()
                        .append("path")
                        .attr("clip-path", "url(#clip)")
                        .attr("d", line)
                        .attr("class", "sequenceLine")
                        .style("z-index", "0")
                        .style("stroke-dasharray", "1,3")
                        .style("stroke-width", "1px")
                        .style("stroke-opacity", 1 );

                    // dottedSeqLine
                    //     .transition()
                    //     .duration(transitionDuration)
                    //     .style("stroke-opacity", 1);
                }
            },
            rectangle: function (object, position) {
                //var rectShift = 20;
                if (!object.height) object.height = 12;
                var rectHeight = object.height;

                var rectShift = rectHeight + rectHeight / 3;
                var lineShift = rectHeight / 2 - 6;
                //                var lineShift = rectHeight/2 - 6;

                var rectsPro = svgContainer.append("g")
                    .attr("class", "rectangle")
                    .attr("clip-path", "url(#clip)")
                    .attr("transform", "translate(0," + position + ")");

                var dataline = [];
                for (var i = 0; i < level; i++) {
                    dataline.push([{
                        x: 1,
                        y: (i * rectShift + lineShift)
                    }, {
                        x: fvLength,
                        y: (i * rectShift + lineShift)
                    }]);
                }
                rectsPro.selectAll(".line" + object.className)
                    .data(dataline)
                    .enter()
                    .append("path")
                    .attr("d", line)
                    .attr("class", function () {
                        return "line" + object.className
                    })
                    .style("z-index", "0")
                    .style("stroke", object.color)
                    .style("stroke-width", "1px");


                var rectsProGroup = rectsPro.selectAll("." + object.className + "Group")
                    .data(object.data)
                    .enter()
                    .append("g")
                    .attr("class", object.className + "Group")
                    .attr("transform", function (d) {
                        return "translate(" + rectX(d) + ",0)"
                    })
                    .on('mouseenter', function(d) {
                        var rw = rectWidth2(d);
                        if (d.descriptionWidth > rw) {
                            d3.select(this)
                                .select("polygon")
                                    .transition(100)
                                    .attr("points", rectPoints(object, d.descriptionWidth + 15))

                            d3.select(this)
                                .select("text")
                                    .attr("x", function (d) {
                                        return (d.descriptionWidth / 2) + 7.5
                                    })
                                    .text(d.description)
                        }
                    })
                    .on("mouseleave", function(d) {
                        var rw = rectWidth2(d);
                        if (d.descriptionWidth > rw) {
                            d3.select(this)
                                .select("polygon")
                                    .transition(100)
                                    .attr("points", rectPoints(object, rw))

                            d3.select(this)
                                .select("text")
                                    .attr("x", rw / 2)
                                    .text(function (d) {
                                        return descriptionElipsis(d, rw - 10);
                                    })
                        }
                    })

                rectsProGroup
                    .append("polygon")
                    .attr("class", "element " + object.className)
                    .attr("id", function (d) {
                        return "f" + d.id
                    })
                    .attr("points", rectPoints(object))
                    .style("fill", function (d) { return d.color || object.color });
                    
                rectsProGroup
                    .append("a")
                    .attr("xlink:xlink:href", function(d) {
                        if (typeof (d.href) === "undefined")
                            return null;
                        return d.href;
                    })
                    .append("text")
                    .attr("class", "element " + object.className + "Text")
                    .attr("y", function (d) {
                        return d.level * rectShift + rectHeight / 2
                    })
                    .attr("x", function (d) {
                        return rectWidth2(d) / 2
                    })
                    .attr("dy", "0.35em")
                    .style("font-size", "10px")
                    .text(function (d) {
                        var rw = rectWidth2(d);
                        if (d.descriptionWidth > rw) {
                            return descriptionElipsis(d, rw - 10);
                        }
                        else {
                            return d.description
                        }
                    })
                    .style("fill", "black")
                    .style("fill", function (d) { return d3.rgb(d.color || object.color).hsl().l < 0.5 ? "white" : "black"; })
                    .style("z-index", "15")
                    .style("text-anchor", "middle")
                    .on("click", function (d) {
                        if (typeof (d.callback) === "undefined")
                            return null;
                        d.callback(d, d3.event);
                    });
                    // .style("visibility", function (d) {
                    //     if (d.description) {
                    //         return (scaling(d.y) - scaling(d.x)) > d.description.length  && rectHeight > 11 ? "visible" : "hidden";
                    //     } else return "hidden";
                    // });
                //     .call(d3.helper.tooltip(object));


                rectsPro.selectAll("." + object.className)
                   .data(object.data)
                   .enter()
                   .append("rect")
                   .attr("clip-path", "url(#clip)")
                   .attr("class", "element "+object.className)
                   .attr("id", function(d) { return "f"+d.id })
                   .attr("x", X)
                   .attr("width", rectWidth)
                   .attr("height", 12)
                   .style("fill", object.color)
                   .style("z-index", "13");
                //    .call(d3.helper.tooltip(object));

                forcePropagation(rectsProGroup);
                var uniqueShift = rectHeight > 12 ? rectHeight - 6 : 0;
                Yposition += level < 2 ? uniqueShift : (level - 1) * rectShift + uniqueShift;
            },
            unique: function (object, position) {
                var rectsPro = svgContainer.append("g")
                    .attr("class", "uniquePosition")
                    .attr("transform", "translate(0," + position + ")");

                var dataline = [];
                dataline.push([{
                    x: 1,
                    y: 0
                }, {
                    x: fvLength,
                    y: 0
                }]);

                rectsPro.selectAll(".line" + object.className)
                    .data(dataline)
                    .enter()
                    .append("path")
                    .attr("clip-path", "url(#clip)")
                    .attr("d", line)
                    .attr("class", "line" + object.className)
                    .style("z-index", "0")
                    .style("stroke", object.color)
                    .style("stroke-width", "1px");

                rectsPro.selectAll("." + object.className)
                    .data(object.data)
                    .enter()
                    .append("rect")
                    .attr("clip-path", "url(#clip)")
                    .attr("class", "element " + object.className)
                    .attr("id", function (d) {
                        return "f" + d.id
                    })
                    .attr("x", function (d) {
                        return scaling(d.x - 0.4)
                    })
                    .attr("width", function (d) {
                        if (scaling(d.x + 0.4) - scaling(d.x - 0.4) < 2) return 2;
                        else return scaling(d.x + 0.4) - scaling(d.x - 0.4);
                    })
                    .attr("height", 12)
                    .style("fill", function (d) { return d.color || object.color })
                    .style("z-index", "3");
                //     .call(d3.helper.tooltip(object));

                forcePropagation(rectsPro);
            },
            path: function (object, position) {
                var pathsDB = svgContainer.append("g")
                    .attr("class", "pathing")
                    .attr("transform", "translate(0," + position + ")");

                var dataline = [];
                dataline.push([{
                    x: 1,
                    y: 0
                }, {
                    x: fvLength,
                    y: 0
                }]);

                pathsDB.selectAll(".line" + object.className)
                    .data(dataline)
                    .enter()
                    .append("path")
                    .attr("clip-path", "url(#clip)")
                    .attr("d", lineBond)
                    .attr("class", "line" + object.className)
                    .style("z-index", "0")
                    .style("stroke", object.color)
                    .style("stroke-width", "1px");

                pathsDB.selectAll("." + object.className)
                    .data(object.data)
                    .enter()
                    .append("path")
                    .attr("clip-path", "url(#clip)")
                    .attr("class", "element " + object.className)
                    .attr("id", function (d) {
                        return "f" + d[0].id
                    })
                    .attr("d", lineBond)
                    .style("fill", "none")
                    .style("stroke", function (d) { return d[0].color || object.color })
                    .style("z-index", "3")
                    .style("stroke-width", "2px");
                //     .call(d3.helper.tooltip(object));

                forcePropagation(pathsDB);
            },
            line: function (object, position) {
                if (!object.interpolation) object.interpolation = "monotone";
                if (object.fill === undefined) object.fill = true;
                var histog = svgContainer.append("g")
                    .attr("class", "lining")
                    .attr("transform", "translate(0," + position + ")");

                var dataline = [];
                dataline.push([{
                    x: 1,
                    y: 0
                }, {
                    x: fvLength,
                    y: 0
                }]);

                histog.selectAll(".line" + object.className)
                    .data(dataline)
                    .enter()
                    .append("path")
                    .attr("clip-path", "url(#clip)")
                    .attr("d", lineBond)
                    .attr("class", "line" + object.className)
                    .style("z-index", "0")
                    .style("stroke", "black")
                    .style("stroke-width", "1px");
                object.data.forEach(function (dd, i, array) {
                    histog.selectAll("." + object.className + i)
                        .data(dd)
                        .enter()
                        .append("path")
                        .attr("clip-path", "url(#clip)")
                        .attr("class", "element " + object.className + " " + object.className + i)
                        .attr("d", lineGen.interpolate(object.interpolation))
                        .style("fill", object.fill ? shadeBlendConvert(0.6, object.color[i]) || shadeBlendConvert(0.6, "#000") : "none")
                        .style("stroke", object.color[i] || "#000")
                        .style("z-index", "3")
                        .style("stroke-width", "2px");
                        //                    .style("shape-rendering", "crispEdges")
                        // .call(d3.helper.tooltip(object));
                })

                forcePropagation(histog);
            },
            multipleRect: function (object, position, level) {
                var rectHeight = 8;
                var rectShift = 10;
                var rects = svgContainer.append("g")
                    .attr("class", "multipleRects")
                    .attr("transform", "translate(0," + position + ")");

                for (var i = 0; i < level; i++) {
                    rects.append("path")
                        .attr("d", line([{
                            x: 1,
                            y: (i * rectShift - 2)
                        }, {
                            x: fvLength,
                            y: (i * rectShift - 2)
                        }]))
                        .attr("class", function () {
                            return "line" + object.className
                        })
                        .style("z-index", "0")
                        .style("stroke", object.color)
                        .style("stroke-width", "1px");
                }

                rects.selectAll("." + object.className)
                    .data(object.data)
                    .enter()
                    .append("rect")
                    .attr("clip-path", "url(#clip)")
                    .attr("class", "element " + object.className)
                    .attr("id", function (d) {
                        return "f" + d.id
                    })
                    .attr("x", X)
                    .attr("y", function (d) {
                        return d.level * rectShift
                    })
                    .attr("width", rectWidth)
                    .attr("height", rectHeight)
                    .style("fill", function (d) { return d.color || object.color })
                    .style("z-index", "13");
                //     .call(d3.helper.tooltip(object));

                forcePropagation(rects);
            }
        };

        function showFilteredFeature(className, color, baseUrl) {
            var featureSelected = yAxisSVG.selectAll("." + className + "Arrow");
            var minY = margin.left - 105;
            var maxY = margin.left - 7;

            var gradient = svg
                .append("linearGradient")
                .attr("y1", "0")
                .attr("y2", "0")
                .attr("x1", minY)
                .attr("x2", maxY)
                .attr("id", "gradient")
                .attr("spreadMethod", "pad")
                .attr("gradientUnits", "userSpaceOnUse");

            gradient
                .append("stop")
                .attr("offset", "0.3")
                .attr("stop-color", "#DFD5D3")
                .attr("stop-opacity", 1);


            var redgrad = gradient
                .append("stop")
                .attr("offset", "1")
                .attr("stop-opacity", 1)
                .attr("stop-color", "#DFD5D3");

            redgrad
                .attr("stop-color", color);

            var url_gradient = "url(#gradient)";
            var url_dropshadow = "url(#dropshadow)";
            if (baseUrl) {
                url_gradient = "url(" + baseUrl + "#gradient)";
                url_dropshadow = "url(" + baseUrl + "#dropshadow)";
            }

            var selection = yAxisSVG.selectAll("." + className + "Arrow")
                .style("fill", url_gradient)
                .style("stroke", "")
                .attr("filter", url_dropshadow);
            selection
                .attr("points", function (d) {
                    return (margin.left - 105) + "," + (d.y - 3) + ", " + (margin.left - 105) + "," + (d.y + 12) + ", " + (margin.left - 10) + "," + (d.y + 12) + ", " + (margin.left - 2) + "," + (d.y + 4.5) + ", " + (margin.left - 10) + "," + (d.y - 3); // x,y points
                });
        };

        var transition = {
            rectangle: function (object) {
                svgContainer.selectAll(".line" + object.className)
                    .attr("d", line.x(function (d) {
                        return scaling(d.x);
                    }));
                var transit1;
                var transit2;
                if (animation) {
                    transit1 = svgContainer.selectAll("." + object.className + "Group")
                        .transition()
                        .duration(transitionDuration);
                    transit2 = svgContainer.selectAll("." + object.className)
                        .transition()
                        .duration(transitionDuration);
                }
                else {
                    transit1 = svgContainer.selectAll("." + object.className + "Group");
                    transit2 = svgContainer.selectAll("." + object.className);
                }
                transit1.attr("transform", function (d) {
                    return "translate(" + rectX(d) + ",0)"
                });

                transit2
                    .attr("points", rectPoints(object))
                transit1.select("text")
                    .attr("x", function(d) { return rectWidth2(d) / 2; })
                    .text(function (d) {
                        var rw = rectWidth2(d);
                        if (d.descriptionWidth > rw) {
                            return descriptionElipsis(d, rw);
                        }
                        else {
                            return d.description
                        }
                    })
                // svgContainer.selectAll("." + object.className + "Text")
                //     .style("visibility", function (d) {
                //         if (d.description) {
                //             return (scaling(d.y) - scaling(d.x)) > d.description.length * 8 && object.height > 11 ? "visible" : "hidden";
                //         } else return "hidden";
                //     });
            },
            multiRec: function (object) {
                svgContainer.selectAll("." + object.className)
                    //                    .data(object.data)
                    .transition()
                    .duration(transitionDuration)
                    .attr("x", function (d) {
                        return scaling(d.x)
                    })
                    .attr("width", function (d) {
                        return scaling(d.y) - scaling(d.x)
                    });
            },
            unique: function (object) {
                svgContainer.selectAll(".line" + object.className)
                    .attr("d", line.x(function (d) {
                        return scaling(d.x);
                    }));
                var transit;
                if (animation) {
                    transit = svgContainer.selectAll("." + object.className)
                        //                    .data(object.data)
                        .transition()
                        .duration(transitionDuration);
                }
                else {
                    transit = svgContainer.selectAll("." + object.className);
                }
                transit
                    //                    .data(object.data)
                    // .transition()
                    // .duration(transitionDuration)
                    .attr("x", function (d) {
                        return scaling(d.x - 0.4)
                    })
                    .attr("width", function (d) {
                        if (scaling(d.x + 0.4) - scaling(d.x - 0.4) < 2) return 2;
                        else return scaling(d.x + 0.4) - scaling(d.x - 0.4);
                    });
            },
            path: function (object) {
                svgContainer.selectAll(".line" + object.className)
                    .attr("d", lineBond.x(function (d) {
                        return scaling(d.x);
                    })
                        .y(function (d) {
                            return -d.y * 10 + object.height;
                        })
                    );
                var transit;
                if (animation) {
                    transit = svgContainer.selectAll("." + object.className)
                        //                    .data(object.data)
                        .transition()
                        .duration(transitionDuration);
                }
                else {
                    transit = svgContainer.selectAll("." + object.className);
                }
                transit
                    .attr("d", lineBond.y(function (d) {
                        return -d.y * 10 + object.height;
                    }));
            },
            line: function (object) {
                lineYscale.range([0, -(object.height)]).domain([0, -(object.level)]);
                svgContainer.selectAll(".line" + object.className)
                    .attr("d", lineGen.y(function (d) {
                        return lineYscale(-d.y) * 10 + object.shift;
                    }));
                var transit;
                if (animation) {
                    transit = svgContainer.selectAll("." + object.className)
                        //                    .data(object.data)
                        .transition()
                        .duration(transitionDuration);
                }
                else {
                    transit = svgContainer.selectAll("." + object.className);
                }

                transit
                    .attr("d", lineGen.y(function (d) {
                        return lineYscale(-d.y) * 10 + object.shift;
                    })
                        .interpolate(object.interpolation)
                    );
            },
            text: function (object, start) {
                var transit;
                if (animation) {
                    transit = svgContainer.selectAll("." + object.className)
                        //                    .data(object.data)
                        .transition()
                        .duration(transitionDuration);
                }
                else {
                    transit = svgContainer.selectAll("." + object.className);
                }
                transit
                    .attr("x", function (d, i) {
                        return scaling(i + start)
                    });
            }
        };

        function addBrush() {
            svgContainer.append("g")
                .attr("class", "brush")
                .call(brush)
                .selectAll("rect")
                .attr('height', Yposition + 50);
        }

        this.resetAll = function () {
            //reset scale
            scaling.domain([offset.start, offset.end]);
            scalingPosition.range([offset.start, offset.end]);
            var seq = displaySequence(offset.end - offset.start);

            if (SVGOptions.showSequence && !(intLength)) {
                if (seq === false && !svgContainer.selectAll(".AA").empty()) {
                    svgContainer.selectAll(".seqGroup").remove();
                    fillSVG.sequenceLine();
                }
                else if (current_extend.length !== fvLength && seq === true && !svgContainer.selectAll(".AA").empty()) {
                    svgContainer.selectAll(".seqGroup").remove();
                    fillSVG.sequence(sequence.substring(offset.start - 1, offset.end), 20, offset.start);
                }
            }

            current_extend = {
                length: offset.end - offset.start,
                start: offset.start,
                end: offset.end
            };
            seqShift = 0;

            transition_data(features, offset.start);
            reset_axis();

            if (typeof (onZoom) == "function") {
                onZoom({
                    start: 1,
                    end: sequence.length,
                    zoom: 1
                });
            }

            d3.select(el).selectAll(".brush").call(brush.clear());
        }

         // If brush is too small, reset view as origin
        function brushend() {
            // debugger;
            d3.select(el).selectAll('div.selectedRect').remove();
            if (featureSelected !== {}) {
                d3.select(featureSelected.id).style("fill", featureSelected.originalColor);
                featureSelected = {};
            }
            // Check if brush is big enough before zooming
            var extent = brush.extent();
            var extentLength = Math.abs(extent[0] - extent[1]);

            var start;
            var end;
            if (extent[0] < extent[1]) {
                start = parseInt(extent[0] - 1);
                end = parseInt(extent[1] + 1);
            } else {
                start = parseInt(extent[1] + 1);
                end = parseInt(extent[0] - 1);
            }

            var seq = displaySequence(extentLength);
            if (!brush.empty() && extentLength > zoomMax) {
                current_extend.length = extentLength;
                var zoomScale = (fvLength / extentLength).toFixed(1);

                //                scaling.range([5,width-5]); 
                if (SVGOptions.showSequence && !(intLength) && seq && svgContainer.selectAll(".AA").empty()) {
                    current_extend = {
                        length: extentLength,
                        start: start,
                        end: end
                    }
                    seqShift = start;
                    svgContainer.selectAll(".sequenceLine").remove();
                    fillSVG.sequence(sequence.substring(start - 1, end), 20, seqShift - 1);
                }

                //modify scale
                //                scaling.range([5,width-5]);
                scaling.domain(extent);
                scalingPosition.range(extent);
                var currentShift = seqShift ? seqShift : offset.start;                

                transition_data(features, currentShift);
                reset_axis();

                if (typeof(onZoom) == "function") {
                    onZoom({
                        start: start,
                        end: end,
                        zoom: zoomScale
                    });
                }

                //rectsPep2.classed("selected", false);
                d3.select(el).selectAll(".brush").call(brush.clear());
            } else {
                d3.select(el).selectAll(".brush").call(brush.clear());
                //resetAll();
            }
        }

        function debounce(func, wait, immediate) {
            var timeout;
            return function() {
                var context = this, args = arguments;
                var later = function() {
                    timeout = null;
                    if (!immediate) func.apply(context, args);
                };
                var callNow = immediate && !timeout;
                clearTimeout(timeout);
                timeout = setTimeout(later, wait);
                if (callNow) func.apply(context, args);
            };
        };

        function updateWindow() {
            if (!svgContainer) return;

            width = el.offsetWidth - margin.left - margin.right - 17;
            d3.select(el).select("svg")
                .attr("width", width + margin.left + margin.right);
            d3.select(el).select("#clip>rect").attr("width", width);
            if (SVGOptions.brushActive) {
                d3.select(el).select(".background").attr("width", width);
            }
            d3.select(el).selectAll(".brush").call(brush.clear());

            //            var currentSeqLength = svgContainer.selectAll(".AA").size();
            var seq = displaySequence(current_extend.length);
            if (SVGOptions.showSequence && !(intLength)) {
                if (seq === false && !svgContainer.selectAll(".AA").empty()) {
                    svgContainer.selectAll(".seqGroup").remove();
                    fillSVG.sequenceLine();
                }
                else if (seq === true && svgContainer.selectAll(".AA").empty()) {
                    svgContainer.selectAll(".sequenceLine").remove();
                    fillSVG.sequence(sequence.substring(current_extend.start - 1, current_extend.end), 20, current_extend.start - 1);

                }
            }

            scaling.range([5, width - 5]);
            scalingPosition.domain([0, width]);

            transition_data(features, current_extend.start);
            reset_axis();
        }

        this.updateWindowDebounced = debounce(updateWindow, 300, false);
        addEventListener("resize", this.updateWindowDebounced, { passive: true });

        function transition_data(features, start) {
            features.forEach(function (o) {
                if (o.type === "rect") {
                    transition.rectangle(o);
                } else if (o.type === "multipleRect") {
                    transition.multiRec(o);
                } else if (o.type === "unique") {
                    transition.unique(o);
                } else if (o.type === "path") {
                    transition.path(o);
                } else if (o.type === "line") {
                    transition.line(o);
                } else if (o.type === "text") {
                    transition.text(o, start);
                }
            });
        }

        /** export to new axis file? */
        function reset_axis() {
            svgContainer
                .transition().duration(transitionDuration)
                .select(".Xaxis-top")
                .call(xAxis1);
            svgContainer
                .transition().duration(transitionDuration)
                .select(".Xaxis")
                .call(xAxis2);
        }

        function addVerticalLine() {
            var vertical = d3.select(".chart")
                .append("div")
                .attr("class", "Vline")
                .style("position", "absolute")
                .style("z-index", "19")
                .style("width", "1px")
                .style("height", (Yposition + 50) + "px")
                .style("top", "30px")
                // .style("left", "0px")
                .style("background", "#000");

            d3.select(".chart")
                .on("mousemove.Vline", function () {
                    var mousex = d3.mouse(this)[0] - 2;
                    vertical.style("left", mousex + "px")
                });
                // .on("click", function(){
                //     mousex = d3.mouse(this);
                //     mousex = mousex[0] + 5;
                //     vertical.style("left", mousex + "px")});
        }

        function addRectSelection(svgId) {
            var featSelection = d3.select(svgId);
            var elemSelected = featSelection.data();
            var xTemp;
            var yTemp;
            var xRect;
            var widthRect;
            var svgWidth = SVGOptions.brushActive ? d3.select(".background").attr("width") : svgContainer.node().getBBox().width;
            d3.select('body').selectAll('div.selectedRect').remove();

            var objectSelected = { type: featSelection[0][0].tagName, color: featSelection.style("fill") };
            colorSelectedFeat(svgId, objectSelected);

            // Append tooltip
            var selectedRect = d3.select(el)
                .append('div')
                .attr('class', 'selectedRect');

            if (elemSelected[0].length === 3) {
                xTemp = elemSelected[0][0].x;
                yTemp = elemSelected[0][1].x;
            } else if (elemSelected[0].x === elemSelected[0].y) {
                xTemp = elemSelected[0].x - 0.5;
                yTemp = elemSelected[0].y + 0.5;
            } else {
                xTemp = elemSelected[0].x;
                yTemp = elemSelected[0].y;
            }
            if (scaling(xTemp) < 0) {
                xRect = margin.left;
                widthRect = (scaling(yTemp));
            } else if (scaling(yTemp) > svgWidth) {
                xRect = scaling(xTemp) + margin.left;
                widthRect = svgWidth - scaling(xTemp);
            } else {
                xRect = scaling(xTemp) + margin.left;
                widthRect = (scaling(yTemp) - scaling(xTemp));
            }
            selectedRect.style({
                left: xRect + 'px',
                top: 60 + 'px',
                'background-color': 'rgba(0, 0, 0, 0.2)',
                width: widthRect + 'px',
                height: (Yposition + 50) + 'px',
                position: 'absolute',
                'z-index': -1,
                'box-shadow': '0 1px 2px 0 #656565'
            });
        };

        function initSVG(options) {
            if (typeof options === 'undefined') {
                options = {
                    'showAxis': false,
                    'showSequence': false,
                    'brushActive': false,
                    'verticalLine': false,
                    'unit': "units",
                    'zoomMax': 50
                }
            }

            // Create SVG
            if (options.zoomMax) {
                zoomMax = options.zoomMax;
            }
            if (!options.unit) {
                options.unit = "units";
            }
            if (options.animation) {
                animation = options.animation;
            }

            width = el.offsetWidth - margin.left - margin.right - 17;
            height = 600 - margin.top - margin.bottom;
            scaling = d3.scale.linear()
                .domain([offset.start, offset.end])
                .range([5, width - 5]);
            scalingPosition = d3.scale.linear()
                .domain([0, width])
                .range([offset.start, offset.end]);

            lineBond = d3.svg.line()
                .interpolate("step-before")
                .x(function (d) {
                    return scaling(d.x);
                })
                .y(function (d) {
                    return -d.y * 10 + pathLevel;
                });
            lineGen = d3.svg.line()
                //          .interpolate("cardinal")
                .x(function (d) {
                    return scaling(d.x);
                })
                .y(function (d) {
                    return lineYscale(-d.y) * 10 + pathLevel;
                });
                
            lineYscale = d3.scale.linear()
                .domain([0, -30])
                .range([0, -20]);

            line = d3.svg.line()
                .interpolate("linear")
                .x(function (d) {
                    return scaling(d.x);
                })
                .y(function (d) {
                    return d.y + 6;
                });

            //Create Axis
            xAxis1 = d3.svg.axis()
                .scale(scaling)
                .tickFormat(d3.format("d"))
                .orient("top");

            xAxis2 = d3.svg.axis()
                .scale(scaling)
                .tickFormat(d3.format("d"))
                .orient("bottom");

            yAxisScale = d3.scale.ordinal()
                .domain([0, yData.length])
                .rangeRoundBands([0, 500], .1);

            yAxis = d3.svg.axis()
                .scale(yAxisScale)
                .tickValues(yData) //specify an array here for values
                .tickFormat(function (d) {
                    return d
                })
                .orient("left");

            brush = d3.svg.brush()
                .x(scaling)
                //.on("brush", brushmove)
                .on("brushend", brushend);
            
            svg = d3.select(el)
                    .style("position", "relative")
                    .style("padding", "0px")
                    .style("z-index", "2")
                        .append("svg")
                            .attr("width", width + margin.left + margin.right)
                            .attr("height", height + margin.top + margin.bottom)
                            .style("z-index", "2")
                // .on("contextmenu", function (d, i) {
                //     d3.event.preventDefault();
                //     resetAll();
                //     // react on right-clicking
                // });
            svgElement = el.getElementsByTagName("svg")[0];

            svgContainer = svg
                .append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

            //Create Clip-Path
            var defs = svgContainer.append("defs");

            defs.append("clipPath")
                .attr("id", "clip")
                .append("rect")
                .attr("width", width)
                .attr("height", height);


            // y-axis labels clip path
            defs.append("clipPath")
                .attr("id", "arrow-clip")
                .append("polygon")
                .attr("points", function (d) {
                    return (margin.left - 110) + "," + (-8) + ", " + (margin.left - 110) + "," + 2 + ", " + (margin.left - 20) + "," + 2 + ", " + (margin.left - 17) + "," + (-5) + ", " + (margin.left - 20) + "," + (-8); // x,y points
                });

            var labelFadeOut = defs.append("linearGradient")
                .attr("id", "label-fade-out")
            
            labelFadeOut.append("stop")
                .attr("offset", "85%")
                .attr("stop-color", "#DFD5D3")
                .attr("stop-opacity", "0")

            labelFadeOut.append("stop")
                .attr("offset", "100%")
                .attr("stop-color", "#DFD5D3")
                .attr("stop-opacity", "1.0")

            var filter = defs.append("filter")
                .attr("id", "dropshadow")
                .attr("height", "200%");

            filter.append("feGaussianBlur")
                .attr("in", "SourceAlpha")
                .attr("stdDeviation", 3)
                .attr("result", "blur");
            filter.append("feOffset")
                .attr("in", "blur")
                .attr("dx", -2)
                .attr("dy", 2)
                .attr("result", "offsetBlur");

            var feMerge = filter.append("feMerge");

            feMerge.append("feMergeNode")
                .attr("in", "offsetBlur");
            feMerge.append("feMergeNode")
                .attr("in", "SourceGraphic");

            svgContainer.on('mousemove', function () {
                var absoluteMousePos = SVGOptions.brushActive ? d3.mouse(d3.select(".background").node()) : d3.mouse(svgContainer.node());;
                var pos = Math.round(scalingPosition(absoluteMousePos[0]));
                if (!options.positionWithoutLetter) {
                    pos += sequence[pos - 1] || "";
                }
            });

            if (typeof options.dottedSequence !== "undefined") {
                SVGOptions.dottedSequence = options.dottedSequence;
            }

            if (options.showSequence && !(intLength)) {
                SVGOptions.showSequence = true;
                if (displaySequence(offset.end - offset.start)) {
                    fillSVG.sequence(sequence.substring(offset.start - 1, offset.end), Yposition, offset.start);
                }
                else {
                    fillSVG.sequenceLine();
                }
                var title = !!id ? id : "Sequence";
                features.push({
                    data: sequence,
                    name: title,
                    className: "AA",
                    color: "black",
                    type: "text"
                });
                yData.push({
                    title: title,
                    y: Yposition - 8
                });
            }

            if (options.verticalLine) {
                SVGOptions.verticalLine = true;
                addVerticalLine();
            }

            if (options.showAxis) {
                svgContainer.append("g")
                    .attr("class", "x axis Xaxis-top")
                    .call(xAxis1);

                svgContainer.append("g")
                    .attr("class", "x axis Xaxis")
                    .attr("transform", "translate(0," + (Yposition + 20) + ")")
                    .call(xAxis2);
            }
            addYAxis();

            if (options.brushActive) {
                SVGOptions.brushActive = true;
                // zoom = true;
                addBrush();
            }

            updateSVGHeight(Yposition);
        }

        this.addFeature = function (object) {
            Yposition += 20;
            features.push(object);
            fillSVG.typeIdentifier(object);
        }

        this.finishRender = function () {
            updateYaxis();
            updateXaxis(Yposition);
            updateSVGHeight(Yposition);
            if (SVGOptions.brushActive) {
                svgContainer.selectAll(".brush rect")
                    .attr('height', Yposition + 50);
            }
            if (SVGOptions.verticalLine) {
                d3.selectAll(".Vline").style("height", (Yposition + 50) + "px");
            }
            if (d3.selectAll(".element")[0].length > 1500) {
                animation = false;
            }
        }

        this.clearInstance = function () {
            removeEventListener("resize", this.updateWindowDebounced);
            svg = null;

            svgContainer.on('mousemove', null)
            svgContainer = null;
            yAxisSVGgroup = null;
            yAxisSVG = null;
            features = null;
            sbcRip = null;

            svgElement.parentElement.removeChild(svgElement);
            svgElement = null;
        }

        initSVG(options);
    }

    return FeatureViewer;
})();

export default FeatureViewer;