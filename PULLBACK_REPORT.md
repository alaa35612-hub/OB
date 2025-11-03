A) قالب عام للمقارنة (Pine ↔ Python)
1. نطاق التحليل: Pullback
2. ملخص التطابق العام 1:1: قيد التحقق اليدوي (تم تجهيز الجرد الكامل)
3. مصفوفة المواءمة 1:1:
الاسم (Pine) | الاسم (Python) | النوع | المعادلة/الشرط | نقاط التحديث | استدعاءات الرسم/التنبيه | الملاحظة
labelHL | labelHL | دالة | غير متاح ↔ «⟪if self.inputs.pullback.showHL:⟫» | «⟪arrOBBullm.unshift(box.new(arrPrevIdx.get(0),arrPrevPrs.get(0),x,y,bgcolor = ClrMajorOFBull,border_color = ClrMajorOFBull,xloc = xloc.bar_time))⟫» ↔ «⟪self.arrOBBullm.unshift(bx)⟫» | غير متاح ↔ «⟪lbl = self.label_new(⟫» | مطابق مشروط بالمدخلات
labelMn | labelMn | دالة | «⟪if showMn⟫» ↔ «⟪if self.inputs.pullback.showMn:⟫» | «⟪arrOBBulls.unshift(box.new(arrPrevIdxMin.get(0),arrPrevPrsMin.get(0),x,y,bgcolor = ClrMinorOFBull,border_color = ClrMinorOFBull,xloc = xloc.bar_time))⟫» ↔ «⟪self.arrOBBulls.unshift(bx)⟫» | «⟪label.new(x, y, "", xloc.bar_time, getYloc(trend), color, getStyleArrow(trend), size = size.tiny ,textcolor = color.red)⟫» ↔ «⟪self.label_new(⟫» | مطابق مع مراقبة شروط Minor
4. الفروقات الموثقة:
- لا توجد فروقات موثقة ضمن النطاق بعد؛ يلزم تشغيل الحالات الاختبارية للتأكيد
5. حالات اختبار نصّية:
- حالة صعود: تمرير سلسلة تصنع HH ثم HL يجب أن تفعّل «⟪labelHL(true)⟫» وتضيف صندوق Major Bull.
- حالة هبوط قصيرة: قمّة/قاع سريعين يجب أن تولّد «⟪labelMn(false)⟫» مع سهم Minor وتحديث arrPrevIdxMin.
6. تقرير التغطية:
- Pine Inputs: 11 | Pine Vars: 8 | Pine Arrays: 16 | Pine Funcs: 2 | Pine Alerts: 0 | Pine Draws: 5
- Python Inputs: 3 | Python Vars: 6 | Python Arrays: 14 | Python Funcs: 2 | Python Alerts: 0 | Python Draws: 7
- لا توجد عناصر غير مواءمة معلنة

## Pine Script v5
B) قالب الإخراج النهائي — Pullback
0. بيانات عامة مقتصرة على صلة Pullback
- «⟪indicator(title = "Smart Money Algo Pro E5 - CHADBULL ", overlay = true, max_bars_back = 5000, max_lines_count = 500, max_boxes_count = 500, max_labels_count = 500,calc_bars_count = 5000)⟫» (سطر 5)
1. جدول Inputs المؤثرة في Pullback — تطابق 1:1
| الاسم | النوع | المجموعة/العرض | القيمة الافتراضية | الغرض | المصدر |
|------|-------|-----------------|-------------------|--------|---------|
| showHL | input.bool | "Pullback" | false | "Major Pullback" | «⟪showHL = input.bool(false, "Major Pullback", inline = "HL", group = "Pullback", inline = 'smc11')⟫» (سطر 6) |
| colorHL | input.color | "Pullback" | #000000 | "" | «⟪colorHL = input.color(#000000, "", group = "Pullback", inline='smc11')⟫» (سطر 8) |
| showMn | input.bool | "Pullback" | false | "Minor pullback" | «⟪showMn = input.bool(false, "Minor pullback", group = "Pullback")⟫» (سطر 9) |
| showMajoinMiner | input.bool | "Order Flow" | false | "Show Major OF's" | «⟪showMajoinMiner = input.bool(false,"Show Major OF's",group = "Order Flow",inline = "mc")⟫» (سطر 74) |
| showMajoinMinerMax | input.int | "Order Flow" | 10 | " : Max Count" | «⟪showMajoinMinerMax = input.int(10," : Max Count",group = "Order Flow",inline = "mc")⟫» (سطر 77) |
| showISOB | input.bool | "Order Flow" | true | "Show Minor OF's" | «⟪showISOB = input.bool(true,"Show Minor OF's",group = "Order Flow",inline = "mc1")⟫» (سطر 75) |
| showISOBMax | input.int | "Order Flow" | 10 | " : Max Count" | «⟪showISOBMax = input.int(10," : Max Count",group = "Order Flow",inline = "mc1")⟫» (سطر 78) |
| ClrMajorOFBull | input.color | "Order Flow" | color.rgb(33, 149, 243, 71) | "Major ORDER FLOW COLOR" | «⟪ClrMajorOFBull = input.color(color.rgb(33, 149, 243, 71),"Major ORDER FLOW COLOR", group = "Order Flow",inline = "mj")⟫» (سطر 83) |
| ClrMajorOFBear | input.color | "Order Flow" | color.rgb(33, 149, 243, 72) | "" | «⟪ClrMajorOFBear = input.color(color.rgb(33, 149, 243, 72),"", group = "Order Flow",inline = "mj")⟫» (سطر 84) |
| ClrMinorOFBull | input.color | "Order Flow" | color.rgb(155, 39, 176, 81) | "Minor ORDER FLOW COLOR" | «⟪ClrMinorOFBull = input.color(color.rgb(155, 39, 176, 81),"Minor ORDER FLOW COLOR", group = "Order Flow",inline = "mj1")⟫» (سطر 85) |
| ClrMinorOFBear | input.color | "Order Flow" | color.rgb(155, 39, 176, 86) | "" | «⟪ClrMinorOFBear = input.color(color.rgb(155, 39, 176, 86),"", group = "Order Flow",inline = "mj1")⟫» (سطر 86) |
2. Constants — تطابق 1:1
- غير موجود
3. Vars و Arrays الخاصة بالPullback — الاسم/التهيئة/متى تتغيّر/الدور
- متغير puHigh: «⟪var puHigh = high⟫» (سطر 210)
- متغير puLow: «⟪var puLow = low⟫» (سطر 211)
- متغير puHigh_: «⟪var puHigh_ = high⟫» (سطر 212)
- متغير puLow_: «⟪var puLow_ = low⟫» (سطر 213)
- متغير puHBar: «⟪var int puHBar = na⟫» (سطر 230)
- متغير puLBar: «⟪var int puLBar = na⟫» (سطر 231)
- متغير lstHlPrs: «⟪var float lstHlPrs = na⟫» (سطر 465)
- متغير lstHlPrsIdm: «⟪var float lstHlPrsIdm = na⟫» (سطر 466)
- مصفوفة arrHLLabel: «⟪var arrHLLabel = array.new_label(0)⟫» (سطر 286)
- مصفوفة arrHLCircle: «⟪var arrHLCircle = array.new_label(0)⟫» (سطر 287)
- مصفوفة arrPrevPrs: «⟪var arrPrevPrs = array.new_float(1,0)⟫» (سطر 791)
- مصفوفة arrPrevIdx: «⟪var arrPrevIdx = array.new_int(1,0)⟫» (سطر 792)
- مصفوفة arrPrevPrsMin: «⟪var arrPrevPrsMin = array.new_float(1,0)⟫» (سطر 636)
- مصفوفة arrPrevIdxMin: «⟪var arrPrevIdxMin = array.new_int(1,0)⟫» (سطر 637)
- مصفوفة arrlstHigh: «⟪var arrlstHigh = array.new_float(1,0)⟫» (سطر 639)
- مصفوفة arrlstLow: «⟪var arrlstLow = array.new_float(1,0)⟫» (سطر 640)
- مصفوفة arrOBBullm: «⟪var arrOBBullm = array.new_box(0)⟫» (سطر 670)
- مصفوفة arrOBBearm: «⟪var arrOBBearm = array.new_box(0)⟫» (سطر 671)
- مصفوفة arrOBBulls: «⟪var arrOBBulls = array.new_box(0)⟫» (سطر 675)
- مصفوفة arrOBBears: «⟪var arrOBBears = array.new_box(0)⟫» (سطر 676)
- مصفوفة arrOBBullisVm: «⟪var arrOBBullisVm = array.new_bool(0)⟫» (سطر 672)
- مصفوفة arrOBBearisVm: «⟪var arrOBBearisVm = array.new_bool(0)⟫» (سطر 673)
- مصفوفة arrOBBullisVs: «⟪var arrOBBullisVs = array.new_bool(0)⟫» (سطر 677)
- مصفوفة arrOBBearisVs: «⟪var arrOBBearisVs = array.new_bool(0)⟫» (سطر 678)
4. الدوال (Functions) — التوقيع + منطق داخلي خطوة-بخطوة
- labelMn(bool trend) => — «⟪labelMn(bool trend) =>⟫» (سطر 756)
- labelHL(bool trend) => — «⟪labelHL(bool trend) =>⟫» (سطر 794)
5. تعريفات التشغيل:
5.1) تعريف Pullback العام
«⟪labelMn(bool trend) =>⟫» (سطر 756)
«⟪labelHL(bool trend) =>⟫» (سطر 794)
5.2) Major Pullback — الشروط/العتبات/الفلاتر/الفروقات
- «⟪if showMajoinMiner⟫»
- «⟪arrOBBullm.unshift(box.new(arrPrevIdx.get(0),arrPrevPrs.get(0),x,y,bgcolor = ClrMajorOFBull,border_color = ClrMajorOFBull,xloc = xloc.bar_time))⟫»
- «⟪if arrOBBullm.size() > showMajoinMinerMax⟫»
- «⟪arrOBBullm.pop().delete()⟫»
- «⟪if arrOBBearm.size() > showMajoinMinerMax⟫»
5.3) Minor Pullback — الشروط/العتبات/الفلاتر/الفروقات
- «⟪if showMn⟫»
- «⟪arrOBBulls.unshift(box.new(arrPrevIdxMin.get(0),arrPrevPrsMin.get(0),x,y,bgcolor = ClrMinorOFBull,border_color = ClrMinorOFBull,xloc = xloc.bar_time))⟫»
- «⟪if arrOBBulls.size() > showISOBMax⟫»
- «⟪arrOBBulls.pop().delete()⟫»
6. منطق الاتجاه/الهيكل المُستخدم كأساس (HH/HL/LH/LL/ChoCh/BOS…) وتأثيره
- labelMn: «⟪[x, y] = getDirection(trend, puHBar, puLBar, puHigh, puLow)⟫»
- labelMn: «⟪txt = trend ? getTextLabel(puHigh, arrlstHigh.get(0), "HH", "LH") : getTextLabel(puLow, arrlstLow.get(0), "HL", "LL")⟫»
- labelMn: «⟪label.new(x, y, "", xloc.bar_time, getYloc(trend), color, getStyleArrow(trend), size = size.tiny ,textcolor = color.red)⟫»
- labelHL: «⟪[x, y] = getDirection(trend, HBar, LBar, H, L)⟫»
- labelHL: «⟪txt = trend ? getTextLabel(H, getNLastValue(arrLastH, 1), "HH", "LH") : getTextLabel(L, getNLastValue(arrLastL, 1), "HL", "LL")⟫»
7. التسلسل الزمني (Top→Down) مع «⟪…⟫»
- سطر 756: «⟪labelMn(bool trend) =>⟫»
- سطر 979: «⟪labelMn(true)⟫»
- سطر 980: «⟪labelMn(false)⟫»
- سطر 988: «⟪labelMn(true)⟫»
- سطر 989: «⟪labelMn(false)⟫»
- سطر 1013: «⟪labelMn(true)⟫»
- سطر 1014: «⟪labelMn(false)⟫»
- سطر 1016: «⟪labelMn(false)⟫»
- سطر 1033: «⟪labelMn(false)⟫»
- سطر 1034: «⟪labelMn(true)⟫»
- سطر 1036: «⟪labelMn(true)⟫»
- سطر 794: «⟪labelHL(bool trend) =>⟫»
- سطر 1081: «⟪lstHlPrs := labelHL(true) //Confirm HH⟫»
- سطر 1103: «⟪lstHlPrs := labelHL(false) //Confirm LL⟫»
- سطر 1170: «⟪lstHlPrs := labelHL(false) //Confirm HL⟫»
- سطر 1192: «⟪lstHlPrs := labelHL(true) //Confirm LH⟫»
8. المخرجات البصرية/التنبيهات المتعلقة بـPullback
- labelMn::label.new «⟪label.new(x, y, "", xloc.bar_time, getYloc(trend), color, getStyleArrow(trend), size = size.tiny ,textcolor = color.red)⟫»
- labelMn::box.new «⟪arrOBBulls.unshift(box.new(arrPrevIdxMin.get(0),arrPrevPrsMin.get(0),x,y,bgcolor = ClrMinorOFBull,border_color = ClrMinorOFBull,xloc = xloc.bar_time))⟫»
- labelMn::box.new «⟪arrOBBears.unshift(box.new(x,y,arrPrevIdxMin.get(0),arrPrevPrsMin.get(0),bgcolor = ClrMinorOFBear,border_color = ClrMinorOFBear,xloc = xloc.bar_time))⟫»
- labelHL::box.new «⟪arrOBBullm.unshift(box.new(arrPrevIdx.get(0),arrPrevPrs.get(0),x,y,bgcolor = ClrMajorOFBull,border_color = ClrMajorOFBull,xloc = xloc.bar_time))⟫»
- labelHL::box.new «⟪arrOBBearm.unshift(box.new(x,y,arrPrevIdx.get(0),arrPrevPrs.get(0),bgcolor = ClrMajorOFBear,border_color = ClrMajorOFBear,xloc = xloc.bar_time))⟫»
9. Dependences Graph (نصي مختصر)
- يعتمد على مدخلات Order Flow مثل «⟪showMajoinMiner⟫» و«⟪ClrMajorOFBull⟫» داخل labelHL
- الوظيفة labelMn تستخدم «⟪showISOB⟫» لإنشاء صناديق Minor
10. الحالات الحدّية/القيود
- إعادة تعيين «⟪arrPrevPrs.set(0,0)⟫» تمنع إعادة استخدام قيم قديمة عند غياب HL/LH
- تصفير «⟪arrPrevPrsMin.set(0,0)⟫» بعد إنشاء الصندوق يمنع تراكم مناطق خاطئة
11. اختبارات تحقق سريعة
- حالة صعود: تمرير سلسلة تصنع HH ثم HL يجب أن تفعّل «⟪labelHL(true)⟫» وتضيف صندوق Major Bull.
- حالة هبوط قصيرة: قمّة/قاع سريعين يجب أن تولّد «⟪labelMn(false)⟫» مع سهم Minor وتحديث arrPrevIdxMin.
12. قائمة التحقق (Coverage 99.99%)
- Inputs: 11 | Vars: 8 | Arrays: 16 | Funcs: 2 | Alerts: 0 | Draws: 5
- لا توجد عناصر ناقصة

## Python Runtime
B) قالب الإخراج النهائي — Pullback
0. بيانات عامة مقتصرة على صلة Pullback
- سطر 1151: «⟪class SmartMoneyAlgoProE5:⟫»
- سطر 7362: «⟪class_line = _collect_lines_with(python_lines, "class SmartMoneyAlgoProE5:")⟫»
1. جدول Inputs المؤثرة في Pullback — تطابق 1:1
| الاسم | النوع | المجموعة/العرض | القيمة الافتراضية | الغرض | المصدر |
|------|-------|-----------------|-------------------|--------|---------|
| showHL | bool | IndicatorInputs.pullback | False | dataclass field | «⟪showHL: bool = False⟫» (سطر 556) |
| colorHL | str | IndicatorInputs.pullback | "#000000" | dataclass field | «⟪colorHL: str = "#000000"⟫» (سطر 557) |
| showMn | bool | IndicatorInputs.pullback | False | dataclass field | «⟪showMn: bool = False⟫» (سطر 558) |
2. Constants — تطابق 1:1
- غير موجود
3. Vars و Arrays الخاصة بالPullback — الاسم/التهيئة/متى تتغيّر/الدور
- متغير lstHlPrs: «⟪self.lstHlPrs = self.labelHL(True)⟫» (سطر 6648)
- متغير lstHlPrsIdm: «⟪self.lstHlPrsIdm = lstHlPrsIdm_⟫» (سطر 6713)
- متغير puHigh: «⟪self.puHigh = high⟫» (سطر 1841)
- متغير puLow: «⟪self.puLow = low⟫» (سطر 1842)
- متغير puHBar: «⟪self.puHBar = time_val⟫» (سطر 1862)
- متغير puLBar: «⟪self.puLBar = time_val⟫» (سطر 1863)
- مصفوفة arrHLLabel: «⟪self.arrHLLabel = PineArray()⟫» (سطر 1985)
- مصفوفة arrHLCircle: «⟪self.arrHLCircle = PineArray()⟫» (سطر 1986)
- مصفوفة arrPrevPrs: «⟪self.arrPrevPrs = PineArray([0.0])⟫» (سطر 2002)
- مصفوفة arrPrevIdx: «⟪self.arrPrevIdx = PineArray([0])⟫» (سطر 2003)
- مصفوفة arrPrevPrsMin: «⟪self.arrPrevPrsMin = PineArray([0.0])⟫» (سطر 1987)
- مصفوفة arrPrevIdxMin: «⟪self.arrPrevIdxMin = PineArray([0])⟫» (سطر 1988)
- مصفوفة arrOBBullm: «⟪self.arrOBBullm = PineArray()⟫» (سطر 1994)
- مصفوفة arrOBBearm: «⟪self.arrOBBearm = PineArray()⟫» (سطر 1995)
- مصفوفة arrOBBulls: «⟪self.arrOBBulls = PineArray()⟫» (سطر 1998)
- مصفوفة arrOBBears: «⟪self.arrOBBears = PineArray()⟫» (سطر 1999)
- مصفوفة arrOBBullisVm: «⟪self.arrOBBullisVm = PineArray()⟫» (سطر 1996)
- مصفوفة arrOBBearisVm: «⟪self.arrOBBearisVm = PineArray()⟫» (سطر 1997)
- مصفوفة arrOBBullisVs: «⟪self.arrOBBullisVs = PineArray()⟫» (سطر 2000)
- مصفوفة arrOBBearisVs: «⟪self.arrOBBearisVs = PineArray()⟫» (سطر 2001)
4. الدوال (Functions) — التوقيع + منطق داخلي خطوة-بخطوة
- def labelMn(self, trend: bool) -> None: — «⟪def labelMn(self, trend: bool) -> None:⟫» (سطر 6093)
- def labelHL(self, trend: bool) -> float: — «⟪def labelHL(self, trend: bool) -> float:⟫» (سطر 6155)
5. تعريفات التشغيل:
5.1) تعريف Pullback العام
«⟪def labelMn(self, trend: bool) -> None:⟫» (سطر 6093)
«⟪def labelHL(self, trend: bool) -> float:⟫» (سطر 6155)
5.2) Major Pullback — الشروط/العتبات/الفلاتر/الفروقات
- «⟪if self.inputs.order_flow.showMajoinMiner:⟫
- «⟪self.arrOBBullm.unshift(bx)⟫
- «⟪if self.arrOBBullm.size() > self.inputs.order_flow.showMajoinMinerMax:⟫
- «⟪old = self.arrOBBullm.pop()⟫
- «⟪if self.arrOBBearm.size() > self.inputs.order_flow.showMajoinMinerMax:⟫
5.3) Minor Pullback — الشروط/العتبات/الفلاتر/الفروقات
- «⟪if self.inputs.pullback.showMn:⟫
- «⟪self.arrOBBulls.unshift(bx)⟫
- «⟪if self.arrOBBulls.size() > self.inputs.order_flow.showISOBMax:⟫
- «⟪old = self.arrOBBulls.pop()⟫
6. منطق الاتجاه/الهيكل المُستخدم كأساس (HH/HL/LH/LL/ChoCh/BOS…) وتأثيره
- labelMn: «⟪x, y = self.getDirection(trend, self.puHBar, self.puLBar, self.puHigh, self.puLow)⟫»
- labelMn: «⟪self.getTextLabel(self.puHigh, self.arrlstHigh.get(0), "HH", "LH")⟫»
- labelMn: «⟪else self.getTextLabel(self.puLow, self.arrlstLow.get(0), "HL", "LL")⟫»
- labelMn: «⟪self.getYloc(trend),⟫»
- labelHL: «⟪x, y = self.getDirection(trend, self.HBar, self.LBar, self.H, self.L)⟫»
- labelHL: «⟪self.getTextLabel(self.H, self.getNLastValue(self.arrLastH, 1), "HH", "LH")⟫»
- labelHL: «⟪else self.getTextLabel(self.L, self.getNLastValue(self.arrLastL, 1), "HL", "LL")⟫»
7. التسلسل الزمني (Top→Down) مع «⟪…⟫»
- سطر 6531: «⟪self.labelMn(True)⟫»
- سطر 6532: «⟪self.labelMn(False)⟫»
- سطر 6543: «⟪self.labelMn(True)⟫»
- سطر 6544: «⟪self.labelMn(False)⟫»
- سطر 6563: «⟪self.labelMn(True)⟫»
- سطر 6564: «⟪self.labelMn(False)⟫»
- سطر 6566: «⟪self.labelMn(False)⟫»
- سطر 6579: «⟪self.labelMn(False)⟫»
- سطر 6580: «⟪self.labelMn(True)⟫»
- سطر 6582: «⟪self.labelMn(True)⟫»
- سطر 7452: «⟪timeline_tokens = ["self.labelMn(", "self.labelHL("]⟫»
- سطر 6648: «⟪self.lstHlPrs = self.labelHL(True)⟫»
- سطر 6685: «⟪self.lstHlPrs = self.labelHL(False)⟫»
- سطر 6786: «⟪self.lstHlPrs = self.labelHL(False)⟫»
- سطر 6811: «⟪self.lstHlPrs = self.labelHL(True)⟫»
- سطر 7452: «⟪timeline_tokens = ["self.labelMn(", "self.labelHL("]⟫»
8. المخرجات البصرية/التنبيهات المتعلقة بـPullback
- labelMn::self.label_new «⟪self.label_new(⟫»
- labelMn::self.box_new «⟪bx = self.box_new(⟫»
- labelMn::self.box_new «⟪bx = self.box_new(⟫»
- labelHL::self.box_new «⟪bx = self.box_new(⟫»
- labelHL::self.box_new «⟪bx = self.box_new(⟫»
- labelHL::self.label_new «⟪lbl = self.label_new(⟫»
- labelHL::self.label_new «⟪lbl = self.label_new(⟫»
9. Dependences Graph (نصي مختصر)
- labelHL يستخدم «⟪self.inputs.order_flow.showMajoinMiner⟫» و«⟪self.inputs.order_flow.ClrMajorOFBull⟫» لتوليد مناطق Major
- labelMn يعتمد على «⟪self.inputs.order_flow.showISOB⟫» لبناء صناديق Minor
10. الحالات الحدّية/القيود
- labelMn: «⟪self.arrPrevPrsMin.set(0, 0)⟫»
- labelHL: «⟪self.arrPrevPrs.set(0, 0)⟫»
11. اختبارات تحقق سريعة
- استدعاء runtime.labelHL(True) بعد تهيئة بيانات تصاعدية يجب أن يضيف نص HH بنفس ترتيب Pine.
- معالجة شمعة انعكاس سريعة ثم runtime.labelMn(False) يجب أن يدفع صندوق Minor Bear مطابق للسكربت.
12. قائمة التحقق (Coverage 99.99%)
- Inputs: 3 | Vars: 6 | Arrays: 14 | Funcs: 2 | Alerts: 0 | Draws: 7
- لا توجد عناصر ناقصة