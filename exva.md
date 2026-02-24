Option Explicit

Sub RepairFilesAndRunPython()

    Dim sourceFolder As String
    Dim cleanedFolder As String
    Dim outputFile As String
    Dim pyExe As String
    Dim pyScript As String
    
    Dim f As String
    Dim wb As Workbook
    Dim srcPath As String
    Dim dstPath As String
    Dim baseName As String
    
    Dim repairedCount As Long, failedCount As Long
    Dim cmd As String
    Dim wsh As Object
    
    ' ==========================
    ' CONFIG - ADAPT THESE PATHS
    ' ==========================
    sourceFolder = "C:\Users\h24826\OneDrive - BNP Paribas\Desktop\Testing for FLABSA"
    cleanedFolder = sourceFolder & "\_cleaned"
    outputFile = sourceFolder & "\impact_synthesis_summary.xlsx"
    
    ' Python interpreter (your venv)
    pyExe = "C:\Users\h24826\OneDrive - BNP Paribas\envs\sf_env\Scripts\python.exe"
    
    ' Python script path (create this file, see section 2 below)
    pyScript = "C:\Users\h24826\OneDrive - BNP Paribas\Desktop\Testing for FLABSA\extract_impact.py"
    
    ' Create cleaned folder if missing
    If Dir(cleanedFolder, vbDirectory) = "" Then MkDir cleanedFolder
    
    Application.ScreenUpdating = False
    Application.DisplayAlerts = False
    Application.EnableEvents = False
    
    repairedCount = 0
    failedCount = 0
    
    ' Process .xlsx
    f = Dir(sourceFolder & "\*.xlsx")
    Do While f <> ""
        If Left$(f, 2) <> "~$" Then
            srcPath = sourceFolder & "\" & f
            baseName = Left$(f, InStrRev(f, ".") - 1)
            dstPath = cleanedFolder & "\" & baseName & "_cleaned.xlsx"
            
            On Error GoTo RepairError
            
            ' Try open with repair mode
            Set wb = Workbooks.Open( _
                Filename:=srcPath, _
                UpdateLinks:=0, _
                ReadOnly:=True, _
                CorruptLoad:=xlRepairFile _
            )
            
            ' Optional recalc before save (if needed)
            ' Application.Calculate
            
            ' Save clean xlsx copy (51 = xlOpenXMLWorkbook)
            wb.SaveAs Filename:=dstPath, FileFormat:=51
            wb.Close SaveChanges:=False
            Set wb = Nothing
            
            repairedCount = repairedCount + 1
            Debug.Print "OK: " & f
            
            On Error GoTo 0
        End If
        
NextFile:
        f = Dir
        DoEvents
    Loop
    
    ' You can repeat for xlsm if needed:
    f = Dir(sourceFolder & "\*.xlsm")
    Do While f <> ""
        If Left$(f, 2) <> "~$" Then
            srcPath = sourceFolder & "\" & f
            baseName = Left$(f, InStrRev(f, ".") - 1)
            dstPath = cleanedFolder & "\" & baseName & "_cleaned.xlsx"
            
            On Error GoTo RepairError2
            
            Set wb = Workbooks.Open( _
                Filename:=srcPath, _
                UpdateLinks:=0, _
                ReadOnly:=True, _
                CorruptLoad:=xlRepairFile _
            )
            
            wb.SaveAs Filename:=dstPath, FileFormat:=51
            wb.Close SaveChanges:=False
            Set wb = Nothing
            
            repairedCount = repairedCount + 1
            Debug.Print "OK: " & f
            
            On Error GoTo 0
        End If
        
NextFile2:
        f = Dir
        DoEvents
    Loop
    
    ' ==========================
    ' LAUNCH PYTHON (WAIT = TRUE)
    ' ==========================
    cmd = """" & pyExe & """ """ & pyScript & """ """ & cleanedFolder & """ """ & outputFile & """"
    
    Set wsh = CreateObject("WScript.Shell")
    ' 1 = normal window, True = wait until python finishes
    wsh.Run cmd, 1, True
    
    MsgBox "Terminé." & vbCrLf & _
           "Fichiers réparés/nettoyés : " & repairedCount & vbCrLf & _
           "Fichiers en échec : " & failedCount & vbCrLf & _
           "Output : " & outputFile, vbInformation
    
CleanExit:
    Application.ScreenUpdating = True
    Application.DisplayAlerts = True
    Application.EnableEvents = True
    Exit Sub

RepairError:
    failedCount = failedCount + 1
    Debug.Print "FAILED (.xlsx): " & srcPath & " | " & Err.Description
    If Not wb Is Nothing Then
        On Error Resume Next
        wb.Close SaveChanges:=False
        Set wb = Nothing
        On Error GoTo 0
    End If
    Err.Clear
    Resume NextFile

RepairError2:
    failedCount = failedCount + 1
    Debug.Print "FAILED (.xlsm): " & srcPath & " | " & Err.Description
    If Not wb Is Nothing Then
        On Error Resume Next
        wb.Close SaveChanges:=False
        Set wb = Nothing
        On Error GoTo 0
    End If
    Err.Clear
    Resume NextFile2

End Sub