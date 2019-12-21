from enums import aligns, unders, highlights, themecolor, colortype, linespacing, tabaligns, leaders

class makehash(object):
    def __init__(self):
        self._dict = dict()

    def addprop(self, name, value):
        self._dict[name] = value
        pass

    def getstr(self):
        strs = []
        for name in self._dict:
            strs.append(name + ':' + self._dict[name])
        return ' '.join(strs)


def make_hash_paragraph(paragraph):
    _hash = makehash()
    _hash.addprop('alignment', make_hash_alignment(paragraph.alignment))
    _hash.addprop('paragraph_format', make_hash_paragraph_format(paragraph.paragraph_format))
    _hash.addprop('runs', make_hash_runs(paragraph.runs))
    _hash.addprop('style', make_hash_para_style(paragraph.style))
    _hash.addprop('text', paragraph.text)
    return _hash.getstr()

def make_hash_alignment(alignment):
    if alignment is None:
        return 'none'
    return aligns[alignment]

def make_hash_runs(runs):
    strs = []
    for run in runs:
        strs.append(make_hash_run(run))
    return '[' + ','.join(strs) + ']'

def make_hash_run(run):
    _hash = makehash()
    _hash.addprop('bold', 'true' if run.bold else 'false')
    _hash.addprop('font', make_hash_font(run.font))
    _hash.addprop('italic', 'true' if run.italic else 'false')
    _hash.addprop('style', make_hash_char_style(run.style))
    _hash.addprop('text', run.text)
    _hash.addprop('underline', make_hash_underline(run.underline))
    return _hash.getstr()

def make_hash_underline(underline):
    if underline is None:
        return 'none'
    elif underline is True:
        return 'true'
    elif underline is False:
        return 'false'
    else:
        return unders[underline]

def make_hash_char_style(style):
    _hash = makehash()
    _hash.addprop('font', make_hash_font(style.font))
    return _hash.getstr()

def make_hash_para_style(style):
    _hash = makehash()
    _hash.addprop('font', make_hash_font(style.font))
    _hash.addprop('paragraph_format', make_hash_paragraph_format(style.paragraph_format))
    return _hash.getstr()

def make_hash_font(font):
    _hash = makehash()
    _hash.addprop('all_caps', 'true' if font.all_caps else 'false')
    _hash.addprop('bold', 'true' if font.bold else 'false')
    _hash.addprop('color', make_hash_color(font.color))
    _hash.addprop('complex_script', 'true' if font.complex_script else 'false')
    _hash.addprop('cs_bold', 'true' if font.cs_bold else 'false')
    _hash.addprop('cs_italic', 'true' if font.cs_italic else 'false')
    _hash.addprop('double_strike', 'true' if font.double_strike else 'false')
    _hash.addprop('emboss', 'true' if font.emboss else 'false')
    _hash.addprop('hidden', 'true' if font.hidden else 'false')
    _hash.addprop('highlight_color', 'none' if font.highlight_color is None else highlights[font.highlight_color])
    _hash.addprop('imprint', 'true' if font.imprint else 'false')
    _hash.addprop('italic', 'true' if font.italic else 'false')
    _hash.addprop('math', 'true' if font.math else 'false')
    _hash.addprop('name', 'none' if font.name is None else font.name)
    _hash.addprop('no_proof', 'true' if font.no_proof else 'false')
    _hash.addprop('outline', 'true' if font.outline else 'false')
    _hash.addprop('rtl', 'true' if font.rtl else 'false')
    _hash.addprop('shadow', 'true' if font.shadow else 'false')
    _hash.addprop('size', 'none' if font.size is None else str(font.size))
    _hash.addprop('small_caps', 'true' if font.small_caps else 'false')
    _hash.addprop('snap_to_grid', 'true' if font.snap_to_grid else 'false')
    _hash.addprop('spec_vanish', 'true' if font.spec_vanish else 'false')
    _hash.addprop('strike', 'true' if font.strike else 'false')
    _hash.addprop('subscript', 'none' if font.subscript is None else 'true' if font.subscript else 'false')
    _hash.addprop('superscript', 'none' if font.superscript is None else 'true' if font.superscript else 'false')
    _hash.addprop('underline', make_hash_underline(font.underline))
    _hash.addprop('web_hidden', 'true' if font.web_hidden else 'false')
    return _hash.getstr()

def make_hash_paragraph_format(pformat):
    _hash = makehash()
    _hash.addprop('alignment', make_hash_alignment(pformat.alignment))
    _hash.addprop('first_line_indent', 'none' if pformat.first_line_indent is None else str(pformat.first_line_indent))
    _hash.addprop('keep_together', 'none' if pformat.keep_together is None else 'true' if pformat.keep_together else 'false')
    _hash.addprop('keep_with_next', 'none' if pformat.keep_with_next is None else 'true' if pformat.keep_with_next else 'false')
    _hash.addprop('left_indent', 'none' if pformat.left_indent is None else str(pformat.left_indent))
    _hash.addprop('line_spacing', 'none' if pformat.line_spacing is None else str(pformat.line_spacing))
    _hash.addprop('line_spacing_rule', 'none' if pformat.line_spacing_rule is None else linespacing[pformat.line_spacing_rule])
    _hash.addprop('page_break_before', 'none' if pformat.page_break_before is None else 'true' if pformat.page_break_before else 'false')
    _hash.addprop('right_indent', 'none' if pformat.right_indent is None else str(pformat.right_indent))
    _hash.addprop('space_after', 'none' if pformat.space_after is None else str(pformat.space_after))
    _hash.addprop('space_before', 'none' if pformat.space_before is None else str(pformat.space_before))
    _hash.addprop('tab_stops', make_hash_tab_stops(pformat.tab_stops))
    _hash.addprop('widow_control', 'none' if pformat.widow_control is None else 'true' if pformat.widow_control else 'false')
    return _hash.getstr()

def make_hash_color(color):
    _hash = makehash()
    _hash.addprop('rgb', 'none' if color.rgb is None else str(color.rgb))
    _hash.addprop('theme_color', 'none' if color.theme_color is None else themecolor[color.theme_color])
    _hash.addprop('type', 'none' if color.type is None else colortype[color.type])
    return _hash.getstr()

def make_hash_tab_stops(tab_stops):
    strs = []
    for stop in tab_stops:
        strs.append(make_hash_tab_stop(stop))
    return '[' + ','.join(strs) + ']'

def make_hash_tab_stop(tab_stop):
    _hash = makehash()
    _hash.addprop('alignment', tabaligns[tab_stop.alignment])
    _hash.addprop('leader', 'spaces' if tab_stop.leader is None else leaders[tab_stop.leader])
    _hash.addprop('position', str(tab_stop.position))
    return _hash.getstr()
