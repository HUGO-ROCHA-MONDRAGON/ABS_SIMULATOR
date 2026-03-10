Sub Replace_And_Merge_On_Both_Workbooks()

    ProcessMILLBA wb_mm.Sheets(1)
    ProcessMILLBA wb.Sheets(1)

End Sub

Sub ProcessMILLBA(ws As Worksheet)

    Dim lastRow As Long, lastCol As Long
    Dim i As Long, j As Long
    Dim dict As Object
    Dim key As String
    Dim firstRow As Long
    
    Set dict = CreateObject("Scripting.Dictionary")
    
    lastRow = ws.Cells(ws.Rows.Count, "A").End(xlUp).Row
    lastCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column
    
    ws.Columns("A").Replace What:="MILLBAREPO", Replacement:="MILLBA", LookAt:=xlWhole
    
    For i = 2 To lastRow
        
        If ws.Cells(i, "A").Value = "MILLBA" Then
            
            key = ws.Cells(i, "A").Value & "|" & CLng(ws.Cells(i, "B").Value)
            
            If Not dict.exists(key) Then
                dict.Add key, i
            Else
                firstRow = dict(key)
                
                For j = 3 To lastCol
                    If IsNumeric(ws.Cells(firstRow, j).Value) And IsNumeric(ws.Cells(i, j).Value) Then
                        ws.Cells(firstRow, j).Value = ws.Cells(firstRow, j).Value + ws.Cells(i, j).Value
                    End If
                Next j
                
                ws.Rows(i).Delete
                i = i - 1
                lastRow = lastRow - 1
            End If
            
        End If
        
    Next i

End Sub