Option Explicit

Sub FilterByTickerPrefix_ColC()
    Const SRC_SHEET As String = "FLABSO"      '<< adapte
    Const DST_SHEET As String = "OUTPUT"      '<< adapte
    Const TICKER As String = "ABCD"           '<< adapte

    Dim wsS As Worksheet, wsD As Worksheet
    Set wsS = ThisWorkbook.Worksheets(SRC_SHEET)
    Set wsD = ThisWorkbook.Worksheets(DST_SHEET)

    'Dernière ligne/col (sur la table)
    Dim lastRow As Long, lastCol As Long
    lastRow = wsS.Cells(wsS.Rows.Count, "C").End(xlUp).Row
    lastCol = wsS.Cells(1, wsS.Columns.Count).End(xlToLeft).Column

    'Clear destination
    wsD.Cells.Clear

    'Copie header (row 1)
    wsS.Range(wsS.Cells(1, 1), wsS.Cells(1, lastCol)).Copy Destination:=wsD.Cells(1, 1)

    Dim r As Long, outR As Long
    outR = 2

    Dim s As String, prefix As String, p As Long

    For r = 2 To lastRow
        s = CStr(wsS.Cells(r, "C").Value)

        If Len(s) > 0 Then
            p = InStr(1, s, " ")
            If p > 0 Then
                prefix = Left$(s, p - 1)
            Else
                prefix = s
            End If

            If StrComp(prefix, TICKER, vbTextCompare) = 0 Then
                wsS.Range(wsS.Cells(r, 1), wsS.Cells(r, lastCol)).Copy Destination:=wsD.Cells(outR, 1)
                outR = outR + 1
            End If
        End If
    Next r
End Sub