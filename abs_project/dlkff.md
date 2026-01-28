Option Explicit

'========================
' PARAMS A ADAPTER
'========================
Private Const SHEET_NAME As String = "FLABSO"   'ou mets ActiveSheet si tu préfères

'Inputs (où se trouvent ORIGN et ctry)
Private Const CELL_ORIGN As String = "N2"
Private Const CELL_CTRY  As String = "N3"

'Outputs lookups (où écrire les résultats des XLOOKUP)
Private Const CELL_OUT_ORIGN_H As String = "O2"  'résultat col H pour ORIGN
Private Const CELL_OUT_CTRY_F  As String = "O3"  'résultat col F pour ctry

'Outputs Top5 (keys + values)
Private Const OUT_TOP5_COUNTRY_KEY As String = "Q2" 'keys (col E)
Private Const OUT_TOP5_COUNTRY_VAL As String = "R2" 'vals (col F)

Private Const OUT_TOP5_ORIGN_KEY   As String = "T2" 'keys (col C)
Private Const OUT_TOP5_ORIGN_VAL   As String = "U2" 'vals (col H)

Private Const HEADER_ROW As Long = 1
Private Const TOPN As Long = 5

'========================
' MAIN
'========================
Public Sub Update_Table_Lookups_And_Top5()
    Dim ws As Worksheet
    Set ws = ThisWorkbook.Worksheets(SHEET_NAME)

    Dim ORIGN As String, ctry As String
    ORIGN = CStr(ws.Range(CELL_ORIGN).Value)
    ctry = CStr(ws.Range(CELL_CTRY).Value)

    '1) ORIGN: lookup C -> H
    ws.Range(CELL_OUT_ORIGN_H).Value = LookupByKey(ws, "C", "H", ORIGN, HEADER_ROW)

    '2) ctry: lookup E -> F
    ws.Range(CELL_OUT_CTRY_F).Value = LookupByKey(ws, "E", "F", ctry, HEADER_ROW)

    '3) Top5 uniques
    WriteTopNUnique ws, keyCol:="E", valCol:="F", headerRow:=HEADER_ROW, _
                    outKeyTopLeft:=ws.Range(OUT_TOP5_COUNTRY_KEY), _
                    outValTopLeft:=ws.Range(OUT_TOP5_COUNTRY_VAL), _
                    N:=TOPN

    WriteTopNUnique ws, keyCol:="C", valCol:="H", headerRow:=HEADER_ROW, _
                    outKeyTopLeft:=ws.Range(OUT_TOP5_ORIGN_KEY), _
                    outValTopLeft:=ws.Range(OUT_TOP5_ORIGN_VAL), _
                    N:=TOPN
End Sub

'========================
' LOOKUP (équivalent XLOOKUP)
'========================
Private Function LookupByKey(ByVal ws As Worksheet, _
                            ByVal keyCol As String, ByVal retCol As String, _
                            ByVal keyValue As String, _
                            ByVal headerRow As Long) As Variant
    If Len(Trim$(keyValue)) = 0 Then
        LookupByKey = vbNullString
        Exit Function
    End If

    Dim lastRow As Long
    lastRow = ws.Cells(ws.Rows.Count, keyCol).End(xlUp).Row
    If lastRow <= headerRow Then
        LookupByKey = vbNullString
        Exit Function
    End If

    Dim rngKeys As Range, m As Variant
    Set rngKeys = ws.Range(ws.Cells(headerRow + 1, keyCol), ws.Cells(lastRow, keyCol))

    m = Application.Match(keyValue, rngKeys, 0)
    If IsError(m) Then
        LookupByKey = vbNullString
    Else
        LookupByKey = ws.Cells(headerRow + CLng(m), retCol).Value
    End If
End Function

'========================
' TOP N UNIQUE (key unique, tri par value desc)
'========================
Private Sub WriteTopNUnique(ByVal ws As Worksheet, _
                           ByVal keyCol As String, ByVal valCol As String, _
                           ByVal headerRow As Long, _
                           ByVal outKeyTopLeft As Range, _
                           ByVal outValTopLeft As Range, _
                           ByVal N As Long)

    Dim lastRow As Long
    lastRow = ws.Cells(ws.Rows.Count, keyCol).End(xlUp).Row
    If lastRow <= headerRow Then Exit Sub

    Dim arrK As Variant, arrV As Variant
    arrK = ws.Range(ws.Cells(headerRow + 1, keyCol), ws.Cells(lastRow, keyCol)).Value2
    arrV = ws.Range(ws.Cells(headerRow + 1, valCol), ws.Cells(lastRow, valCol)).Value2

    Dim dict As Object
    Set dict = CreateObject("Scripting.Dictionary") 'late binding (pas besoin de référence)

    Dim i As Long, k As String, v As Double
    For i = 1 To UBound(arrK, 1)
        k = CStr(arrK(i, 1))
        If Len(Trim$(k)) > 0 Then
            If IsNumeric(arrV(i, 1)) Then
                v = CDbl(arrV(i, 1))
            Else
                v = 0#
            End If

            'sans doublons : on garde 1 valeur par key (si répétée, on prend le max par sécurité)
            If Not dict.Exists(k) Then
                dict.Add k, v
            Else
                If v > CDbl(dict(k)) Then dict(k) = v
            End If
        End If
    Next i

    'clear output
    outKeyTopLeft.Resize(N, 1).Value = vbNullString
    outValTopLeft.Resize(N, 1).Value = vbNullString

    If dict.Count = 0 Then Exit Sub

    Dim keys() As Variant, vals() As Double
    ReDim keys(1 To dict.Count)
    ReDim vals(1 To dict.Count)

    Dim idx As Long: idx = 0
    Dim kk As Variant
    For Each kk In dict.Keys
        idx = idx + 1
        keys(idx) = kk
        vals(idx) = CDbl(dict(kk))
    Next kk

    QuickSortPairs vals, keys, 1, UBound(vals)

    Dim m As Long: m = WorksheetFunction.Min(N, UBound(vals))

    Dim outK() As Variant, outV() As Variant
    ReDim outK(1 To m, 1 To 1)
    ReDim outV(1 To m, 1 To 1)

    For i = 1 To m
        outK(i, 1) = keys(i)
        outV(i, 1) = vals(i)
    Next i

    outKeyTopLeft.Resize(m, 1).Value = outK
    outValTopLeft.Resize(m, 1).Value = outV
End Sub

'========================
' QUICKSORT (desc) sur vals(), en permutant keys() en parallèle
'========================
Private Sub QuickSortPairs(ByRef vals() As Double, ByRef keys() As Variant, ByVal lo As Long, ByVal hi As Long)
    Dim i As Long, j As Long
    Dim pivot As Double
    Dim tmpV As Double, tmpK As Variant

    i = lo: j = hi
    pivot = vals((lo + hi) \ 2)

    Do While i <= j
        Do While vals(i) > pivot: i = i + 1: Loop  'DESC
        Do While vals(j) < pivot: j = j - 1: Loop  'DESC

        If i <= j Then
            tmpV = vals(i): vals(i) = vals(j): vals(j) = tmpV
            tmpK = keys(i): keys(i) = keys(j): keys(j) = tmpK
            i = i + 1: j = j - 1
        End If
    Loop

    If lo < j Then QuickSortPairs vals, keys, lo, j
    If i < hi Then QuickSortPairs vals, keys, i, hi
End Sub