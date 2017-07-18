from __future__ import print_function

from bokeh.util.browser import view
from bokeh.document import Document
from bokeh.embed import file_html
from bokeh.models.glyphs import Circle
from bokeh.models import (
    GMapPlot, Range1d, ColumnDataSource, PanTool,HoverTool, WheelZoomTool, BoxSelectTool, GMapOptions)
from bokeh.resources import INLINE
from collections import OrderedDict

def plot(lats, lons,count):
    x_range = Range1d()
    y_range = Range1d()

    # JSON style string taken from: https://snazzymaps.com/style/1/pale-dawn
    map_options = GMapOptions(lat=51.5034091928929, lng=-0.151864316576549, map_type="roadmap", zoom=5, styles="""
    [{"featureType":"administrative","elementType":"all","stylers":[{"visibility":"on"},{"lightness":33}]},{"featureType":"landscape","elementType":"all","stylers":[{"color":"#f2e5d4"}]},{"featureType":"poi.park","elementType":"geometry","stylers":[{"color":"#c5dac6"}]},{"featureType":"poi.park","elementType":"labels","stylers":[{"visibility":"on"},{"lightness":20}]},{"featureType":"road","elementType":"all","stylers":[{"lightness":20}]},{"featureType":"road.highway","elementType":"geometry","stylers":[{"color":"#c5c6c6"}]},{"featureType":"road.arterial","elementType":"geometry","stylers":[{"color":"#e4d7c6"}]},{"featureType":"road.local","elementType":"geometry","stylers":[{"color":"#fbfaf7"}]},{"featureType":"water","elementType":"all","stylers":[{"visibility":"on"},{"color":"#acbcc9"}]}]
""")

    API_KEY = "AIzaSyBIQD3thSiRfIw1FRJcSbr0-GBo6Yv6r8Q"

    plot = GMapPlot(
        x_range=x_range, y_range=y_range,
        map_options=map_options,
        api_key=API_KEY,
    )
    plot.title.text = "Property Sales Transaction Clusters for 2010"

    source = ColumnDataSource(
        data=dict(
            lat=lats,
            lon=lons,
            count = count,           
        )
    )

    circle = Circle(x="lon", y="lat", size=15, fill_color="green", line_color="black")
    plot.add_glyph(source, circle)

    pan = PanTool()
    wheel_zoom = WheelZoomTool()
    box_select = BoxSelectTool()
    hover_tool = HoverTool()

    plot.add_tools(pan, wheel_zoom, box_select,hover_tool)
    hover = plot.select(dict(type=HoverTool))
    hover.tooltips = OrderedDict([("# Transactions:","@count")])

    doc = Document()
    doc.add_root(plot)
    

    filename = "maps.html"
    with open(filename, "w") as f:
        f.write(file_html(doc, INLINE, "UK home sales"))
    print("Wrote %s" % filename)
    #view(filename)
