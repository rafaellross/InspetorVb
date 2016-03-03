Imports System.Data.SqlClient
Imports System.IO
Imports System.Data.Odbc

Module Module1
    Public Function GetConnectionXML() As SqlConnection
        'Recebe as variaveis para conectar no BD com as regras de validacoes do XML

        Dim strUsuarioBanco, strSenhaUsuarioBanco, strBanco, strServidor As String
        Dim configurationAppSettings As System.Configuration.AppSettingsReader = New System.Configuration.AppSettingsReader()
        strUsuarioBanco = configurationAppSettings.GetValue("userBD", GetType(System.String))
        strSenhaUsuarioBanco = configurationAppSettings.GetValue("passBD", GetType(System.String))
        strBanco = configurationAppSettings.GetValue("DBXML", GetType(System.String))
        strServidor = configurationAppSettings.GetValue("Server", GetType(System.String))
        'Obtem a string de conexão 
        Dim sConnString As String =
            "Data Source = " + strServidor + "; Initial Catalog = " + strBanco + "; User Id =" + strUsuarioBanco + "; Password =" + strSenhaUsuarioBanco + ";Pooling=False;"
        'Retorna uma conexão.
        Return New SqlConnection(sConnString)
    End Function

    Public Function GetConnectionERP() As SqlConnection
        'Recebe as variaveis para conectar no BD do sistema ERP
        Dim strUsuarioBanco, strSenhaUsuarioBanco, strBanco, strServidor As String
        Dim configurationAppSettings As System.Configuration.AppSettingsReader = New System.Configuration.AppSettingsReader()
        strUsuarioBanco = configurationAppSettings.GetValue("userBD", GetType(System.String))
        strSenhaUsuarioBanco = configurationAppSettings.GetValue("passBD", GetType(System.String))
        strBanco = configurationAppSettings.GetValue("DBERP", GetType(System.String))
        strServidor = configurationAppSettings.GetValue("Server", GetType(System.String))
        'Obtem a string de conexão 
        Dim sConnString As String =
            "Data Source = " + strServidor + "; Initial Catalog = " + strBanco + "; User Id =" + strUsuarioBanco + "; Password =" + strSenhaUsuarioBanco + ";Pooling=False;"
        'Retorna uma conexão.
        Return New SqlConnection(sConnString)
    End Function

    Public Function GetConnectionDBF() As OdbcConnection
        'Recebe as variaveis para conectar no BD DBF

        'oConn.ConnectionString = "Driver={Microsoft dBase Driver (*.dbf)};SourceType=DBF;SourceDB=" & sSigamatDest & ";Exclusive=No;Collate=Machine;NULL=NO;DELETED=NO;BACKGROUNDFETCH=NO;"

        Dim sSigamatDest As String
        Dim configurationAppSettings As System.Configuration.AppSettingsReader = New System.Configuration.AppSettingsReader()
        sSigamatDest = configurationAppSettings.GetValue("DestinoSigaMat", GetType(System.String))
        'Obtem a string de conexão 
        Dim sConnString As String = _
            "Driver={Microsoft dBase Driver (*.dbf)};SourceType=DBF;SourceDB=" & sSigamatDest & ";Exclusive=No;Collate=Machine;NULL=NO;DELETED=NO;BACKGROUNDFETCH=NO;"
        'Retorna uma conexão.
        Return New OdbcConnection(sConnString)
    End Function

    Function fConverteCnpj(ByVal sCnpj As String) As String
        Dim sAux As String
        Dim sResp As String

        sAux = Strings.Right(sCnpj, 2)
        sResp = "-" & sAux
        sAux = Mid(sCnpj, 9, 4)
        sResp = "/" & sAux & sResp
        sAux = Mid(sCnpj, 6, 3)
        sResp = "." & sAux & sResp
        sAux = Mid(sCnpj, 3, 3)
        sResp = "." & sAux & sResp
        sAux = Strings.Left(sCnpj, 2)
        sResp = sAux & sResp
        Return sResp
    End Function

    Function fConverteCpf(ByVal sCpf As String) As String
        Dim sAux As String
        Dim sResp As String

        sAux = Strings.Right(sCpf, 2)
        sResp = "-" & sAux
        sAux = Mid(sCpf, 7, 3)
        sResp = "." & sAux & sResp
        sAux = Mid(sCpf, 4, 3)
        sResp = "." & sAux & sResp
        sAux = Mid(sCpf, 1, 3)
        sResp = sAux & sResp
        Return sResp
    End Function

    Function fDesconverteCNPJ(ByVal sCnpj As String) As String
        Dim sAux As String
        sAux = sCnpj.Replace(".", "")
        sAux = sAux.Replace("/", "")
        sAux = sAux.Replace("-", "")
        Return sAux
    End Function

    Function fvalXmlProcessado(ByVal sChaveNfe As String, ByVal sCodTabEmitDest As String) As String
        Dim bVal As Boolean = False
        Using con As SqlConnection = GetConnectionERP()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT F2_CHVNFE FROM SF2" & sCodTabEmitDest & "0 WHERE F2_CHVNFE = '" & sChaveNfe & "'  AND D_E_L_E_T_ <> '*'"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                dr.Read()
                If dr.HasRows Then
                    bVal = True
                Else
                    bVal = False
                End If
                con.Dispose()
            Catch ex As Exception
                bVal = False
            End Try
        End Using
        Return bVal
    End Function

    Function fvalXmlProcessadoTransf(ByVal sChaveNfe As String, ByVal sCodTabEmitDest As String) As String

        Dim bVal As Boolean = False
        Using con As SqlConnection = GetConnectionERP()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT F1_CHVNFE FROM SF1" & sCodTabEmitDest & "0 WHERE F1_CHVNFE = '" & sChaveNfe & "'  AND D_E_L_E_T_ <> '*'"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                dr.Read()
                If dr.HasRows Then
                    bVal = True
                Else
                    bVal = False
                End If
                con.Dispose()
            Catch ex As Exception
                bVal = False
            End Try
        End Using
        Return bVal
    End Function

    Sub fLog(ByVal sXMLName As String, ByVal sXMLDesc As String)
        'Variáveis para Log
        Dim dDataAtual As String = DateTime.Now.ToString("yyyyMMdd")
        Dim sPath As String = ("C:\Inspetor\Log")
        Dim sPathLog As FileInfo = New FileInfo("C:\Inspetor\Log\InspetorXML.log")
        Dim sw As StreamWriter

        'Verifica se existe caminho do log
        If (Not System.IO.Directory.Exists(sPath)) Then
            System.IO.Directory.CreateDirectory(sPath)
        End If

        'Se o arquivo de log foi criado, salva variaveis da função no arquivo
        If sPathLog.Exists = True Then
            sw = sPathLog.AppendText()
        Else : sw = sPathLog.CreateText()
        End If

        sw.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") & " --- " & sXMLDesc & " --- " & sXMLName)
        sw.Flush()
        sw.Close()
    End Sub
End Module
