
Imports System.IO
Imports System.Timers
Imports System.Data.SqlClient
Imports System
Imports System.Xml
Imports System.Xml.XPath
Imports System.Math
Imports System.Net.Mail
Imports System.Text
Imports System.Globalization
Imports System.Data.Odbc
Imports System.ServiceProcess

Public Class Service_InspetorXML : Inherits ServiceBase



    Dim bAtivaLog As Boolean = True
    Dim bAtivaEmail As Boolean = False
    Protected tempo As Timer

    Public Sub New()
        MyBase.New()
        CanPauseAndContinue = True
        tempo = New Timer
    End Sub

    Dim configurationAppSettings As System.Configuration.AppSettingsReader = New System.Configuration.AppSettingsReader()
    Dim sCodTabEmitDest, sCodEmpEmitDest, sFilialEmitDest, sCnpjEmitDest, sNomeEmitDest, sUFEmitDest,
        sFilialDest, sFilialEmit, sCodEmpDest, sCodEmpEmit, sLojaDest, sLojaEmit, sCnpjDest, sCnpjEmit, sCodTabDest, sCodTabEmit,
        sNomeEmit, sNomeDest, sUFEmit, sUFDest, sTipoCli, sSC5Num As String
    Dim sCstRegra, sComCfopRegra, sCupomFiscalRegra, sSitMercRevRegra, sSitMercUsoRegra, sSitMercAtivoRegra, sOutrosMercServRegra, sMercRevComStRegra, sMercRevSemStRegra, sUsoComStRegra,
        sUsoSemStRegra, sRetConsertRegra, sAtivoComStRegra, sAtivoSemStRegra, sFilialSCRegra, sFilialSC1Regra, sFilialCDRegra, sIssRegra, sCstComReducao, sCstComAliqIcms As String
    Dim sCodUsuario, sCodPrd, sDataEmissao, sDataSaida, sDFEst, caminho, sProcessado, sTpNf, sCodCCusto, sLogo, sManual, sCriticados, sEnvioHoraDiario1, sEnvioHoraDiario2, sEnvioHoraDiario3,
        sEnvioHoraDiario4, sFilialTemp, arquivoxml, sLocEst, sSigamatOrig, sSigamatDest, TabSb1, prodNCM As String

    Dim iIdDoc, iSitAtu, iQuantPo As Integer
    Dim bValPoOk, bErro, bValXml, bEnviaEmail, bValEnv, bValXmlLanc As Boolean

    'Array com 2 posicao (String) --> Cod. Produto | Pedido -- PO
    Dim mPoString(4, 0) As String
    'Array com 2 posicao (Double) --> Preco | Quant --  PO
    Dim mPoDouble(1, 0) As Double

    Dim oReader As StreamReader

    Dim arq As System.IO.FileInfo

    Protected Overrides Sub OnStart(ByVal args() As String)
        Dim sTempo As String
        sTempo = configurationAppSettings.GetValue("TempoExec", GetType(System.String))
        AddHandler tempo.Elapsed, AddressOf OnElapsedTime
        tempo.Interval = Str(sTempo)
        tempo.Enabled = True
        fLog("Função:", "OnStart")
    End Sub


    Public Sub OnElapsedTime(ByVal source As Object, ByVal e As ElapsedEventArgs)


        tempo.Enabled = False

        InspetorXML.My.Application.ChangeCulture("pt-BR")

        'Carrega Parâmetros
        CarregaApp(e)

        fLog("Arquivos", caminho)
        Dim dir As New System.IO.DirectoryInfo(caminho)

        'Verifica quantos arquivos XMLs possui no repostório.
        For Each Me.arq In dir.GetFiles("*.xml")

            'Se bAtivaLog for TRUE salva informação no Log

            If bAtivaLog Then
                fLog(arq.Name, "Iniciando XML")
            End If

            'Cria uma instância de um documento XML
            fLog(arq.Name, "Cria uma instância de um documento XML")
            iQuantPo = 0
            bValXml = True
            bEnviaEmail = False
            bValPoOk = False
            bErro = False
            bValEnv = False
            bValXmlLanc = False

            'Cria uma instância de um documento XML
            fLog(arq.Name, "Cria uma instância de um documento XML")
            Dim xmlDoc As New XmlDocument
            Dim ns As New XmlNamespaceManager(xmlDoc.NameTable)
            ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe")
            Dim xpathNav As XPathNavigator = xmlDoc.CreateNavigator()
            Dim node As XPathNavigator

            'Caminho onde se encontra os xmls + nome do xml que está sendo tratado.
            arquivoxml = caminho & "\" & arq.Name

            bValEnv = fVerificaXmlCriticado(arq.Name.ToString, e)

            If bValEnv Then
                fLog(arq.Name, "XML Criticado!") 'Apagar
                If bAtivaLog Then
                    fLog(arq.Name, "XML Criticado!")
                End If
                Continue For
            End If

            Dim xDoc As XDocument

            Try

                If bAtivaLog Then
                    fLog(arq.Name, "Valida XML")
                End If

                oReader = New StreamReader(arquivoxml, Encoding.GetEncoding("ISO-8859-1")) 'Converte o XML para o encond ("ISO-8859-1") possibilitando ler arquivos XML que possuem acentos em seus textos
                'System.Text.Encoding.ASCII.GetString(System.Text.Encoding.ASCII.GetBytes(oReader.ReadToEnd()))
                xDoc = XDocument.Load(oReader)
            Catch ex As Exception
                fLog(arq.Name, "Erro Valida XML" & ex.ToString())
                Dim sMsgErro As String
                sMsgErro = ex.Message
                If sMsgErro.Length >= 37 Then
                    sMsgErro = sMsgErro.Substring(0, 37)
                End If
                If (sMsgErro = "Elemento raiz inexistente.") Or (sMsgErro = "O processo não pode acessar o arquivo") Or (sMsgErro = "Root element is missing.") Then
                    oReader.Close()
                    If bAtivaLog Then
                        fLog(arq.Name, "Final do Processo 98")
                    End If
                    Continue For
                End If
                Using con As SqlConnection = GetConnectionXML()  'Grava log
                    Try
                        fLog(arq.Name, "Abriu banco Logeventos")
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO LOGEVENTOSXML (NOME_XML, DATAEMISSAO, SETOR, USUARIO, EVENTO, CRITICA) " +
                            "VALUES ('" & arq.Name & "',convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), 'FIS', '" & sCodUsuario & "', 'I', 'XML TRANSFERIDO PARA A PASTA CRITICADOS, O MESMO ENCONTRA-SE CORROMPIDO. MENSAGEM DE ERRO: " & sMsgErro & "')"
                        cmd.ExecuteReader()
                    Catch ex2 As Exception
                        fLog(arq.Name, "Erro Logeventos" & ex.ToString())
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex2.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        If bAtivaLog Then
                            fLog(arq.Name, "Final do Processo 118")
                        End If
                        Continue For
                    End Try
                    con.Dispose()
                End Using
                oReader.Close()
                File.Delete(sCriticados & "\" & arq.Name)
                File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                File.Delete(caminho & "\" & arq.Name)
                If bAtivaLog Then
                    fLog(arq.Name, "Final do Processo 129")
                End If
                Continue For
            End Try

            xmlDoc.Load(New StringReader(fRemoverAcentos(xDoc.ToString))) 'Função que remove acentos

            fLeRegras(e)

            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:emit/nfe:CNPJ", ns)
            If node Is Nothing Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:emit/nfe:CPF", ns)
                sCnpjEmit = node.InnerXml.ToString 'Cnpj do Emitente da NF
            Else
                sCnpjEmit = node.InnerXml.ToString 'Cnpj do Emitente da NF
            End If
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:emit/nfe:xNome", ns)
            sNomeEmit = node.InnerXml.ToString 'Razao Social do Fornecedor
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:emit/nfe:enderEmit/nfe:UF", ns)
            sUFEmit = node.InnerXml.ToString
			
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:dest/nfe:CNPJ", ns)
            If node Is Nothing Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:dest/nfe:CPF", ns)
                sCnpjDest = node.InnerXml.ToString 'Cnpj do Destinatario da NF
            Else
                sCnpjDest = node.InnerXml.ToString 'Cnpj do Destinatario da NF
            End If
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:dest/nfe:xNome", ns)
            sNomeDest = node.InnerXml.ToString 'Razao Social do Fornecedor
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:dest/nfe:enderDest/nfe:UF", ns)
            sUFDest = node.InnerXml.ToString
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:dhEmi", ns)
            Dim dtEmi As String = node.InnerXml.ToString

            sDataEmissao = dtEmi.Substring(0, 19)

            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:emit/nfe:CRT", ns)
            iSitAtu = node.InnerXml.ToString
            sTpNf = FValidaTipoNf(caminho, arq, xmlDoc, e)

            Dim sChaveNfe As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe", ns)
            sChaveNfe = Strings.Right(node.GetAttribute("Id", ""), 44)       'Chave de acesso da NF-e

            If fValidaNotaLancada(sChaveNfe, e) Then
                oReader.Close()
                If bAtivaLog Then
                    fLog(arq.Name, "Final do Processo 173")
                End If
                Continue For
            End If

            If sTpNf = "TRANSF_ENTSAI" Or sTpNf = "SAIDA" Then
                sCodTabEmitDest = sCodTabEmit
                sCnpjEmitDest = sCnpjEmit
                sCodEmpEmitDest = sCodEmpDest
                sUFEmitDest = sUFDest
                sFilialEmitDest = sFilialEmit
                sNomeEmitDest = sNomeDest
            ElseIf sTpNf = "TRANSF_ENT" Or sTpNf = "ENTRADA" Then
                sCodTabEmitDest = sCodTabDest
                sCnpjEmitDest = sCnpjDest
                sCodEmpEmitDest = sCodEmpEmit
                sUFEmitDest = sUFEmit
                sFilialEmitDest = sFilialDest
                sNomeEmitDest = sNomeEmit
            ElseIf sTpNf = "ENT_IMP" Then
                sCodTabEmitDest = sCodTabDest
                sCnpjEmitDest = sCnpjEmit
                sCodEmpEmitDest = sCodEmpDest
                sUFEmitDest = sUFDest
                sFilialEmitDest = sFilialDest
                sNomeEmitDest = sNomeEmit
            ElseIf sTpNf = "N/A" Then
                Using con As SqlConnection = GetConnectionXML()  'Grava log
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO LOGEVENTOSXML (NOME_XML, DATAEMISSAO, SETOR, USUARIO, EVENTO, CRITICA) " +
                            "VALUES ('" & arq.Name & "',convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), 'FIS', '" & sCodUsuario & "', 'I', " +
                            "'XML TRANSFERIDO PARA A PASTA CRITICADOS, O MESMO NAO PERTENCE AO GRUPO DA EMPRESA')"
                        cmd.ExecuteReader()
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                    End Try
                    con.Dispose()
                End Using
                oReader.Close()
                File.Delete(sCriticados & "\" & arq.Name)
                File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                File.Delete(caminho & "\" & arq.Name)
                If bAtivaLog Then
                    fLog(arq.Name, "Final do Processo 222")
                End If
                Continue For
            End If

            If bAtivaLog Then
                fLog(arq.Name, "Tipo NF " + sTpNf)
            End If

            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[1]/nfe:prod/nfe:CFOP", ns)
            sDFEst = node.InnerXml.Substring(0, 1)
            Dim sCompCfop As String
            sCompCfop = node.InnerXml.Substring(1, 3)

            If sTpNf = "ENTRADA" Or sTpNf = "TRANSF_ENT" Then
                Select Case sDFEst
                    Case "1"
                        sDFEst = "5"
                    Case "2"
                        sDFEst = "6"
                    Case "3"
                        sDFEst = "7"
                    Case "5"
                        sDFEst = "1"
                    Case "6"
                        sDFEst = "2"
                    Case "7"
                        sDFEst = "3"
                End Select
            End If
            'Dim iValidaPo As Integer
            'Using con As SqlConnection = GetConnectionXML()
            '    Try
            '        con.Open()
            '        Dim cmd As New SqlCommand
            '        cmd.Connection = con
            '        cmd.CommandText = "SELECT ATIVO FROM REGRASXML WHERE TIPO_VALIDACAO = 'PEDIDOCOMPRA' AND ORDEM = 1"
            '        Dim dr As SqlDataReader = cmd.ExecuteReader()
            '        dr.Read()
            '        If dr.HasRows Then
            '            iValidaPo = dr.Item(0) * -1
            '        End If
            '    Catch ex As Exception
            '        Dim cmd As New SqlCommand
            '        cmd.Connection = con
            '        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
            '        cmd.ExecuteReader()
            '        oReader.Close()
            '        con.Dispose()
            '        Continue For
            '    End Try
            '    con.Dispose()
            'End Using

            Using con As SqlConnection = GetConnectionXML()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    Dim dr As SqlDataReader
                    cmd.CommandText = "SELECT DISTINCT (FLAG_STATUS) FROM CRITICAXML WHERE (FLAG_STATUS = 'E' OR FLAG_STATUS = 'I' OR FLAG_STATUS IS NULL) AND NOME_XML = '" & arq.Name & "'"
                    dr = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        While dr.Read()
                            If dr.Item(0).ToString = "ENTRADA" Or dr.Item(0).ToString = "" Then
                                bValXml = False
                            End If
                        End While
                    End If
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    If bAtivaLog Then
                        fLog(arq.Name, "Final do Processo 300")
                    End If
                    Continue For
                End Try
                con.Dispose()
            End Using

            If bValXml Then
                Dim bXmlInvalido As Boolean = True
                If sTpNf = "N/A" Then
                    bXmlInvalido = False
                    Using con As SqlConnection = GetConnectionXML()
                        Try
                            con.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CNPJ, RAZAO, CRITICA, TIPO) VALUES " +
                                "('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', 'CMP', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', " +
                                "'NENHUM DOS CNPJS (EMITENTE E DESTINATARIO) CONSTA NO CADASTRO DO SISTEMA, XML INVALIDO', " +
                                "'C')"
                            cmd.ExecuteReader()
                        Catch ex As Exception
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            con.Dispose()
                            If bAtivaLog Then
                                fLog(arq.Name, "Final do Processo 329")
                            End If
                            Continue For
                        End Try
                        con.Dispose()
                    End Using
                End If

                If bXmlInvalido Then
                    If fVerCadCliFor(e) Then
                        If fLeItensNfe(caminho, arq, xmlDoc, e) Then
                            PopulaTabelas(caminho, arq, xmlDoc, e)
                            If sTpNf = "TRANSF_ENTSAI" Then
                                sTpNf = "TRANSF_ENT"
                                sCodTabEmitDest = sCodTabDest
                                sCnpjEmitDest = sCnpjDest
                                sCodEmpEmitDest = sCodEmpEmit
                                sUFEmitDest = sUFEmit
                                sFilialEmitDest = sFilialDest
                                sNomeEmitDest = sNomeEmit
                                Select Case sDFEst
                                    Case "1"
                                        sDFEst = "5"
                                    Case "2"
                                        sDFEst = "6"
                                    Case "3"
                                        sDFEst = "7"
                                    Case "5"
                                        sDFEst = "1"
                                    Case "6"
                                        sDFEst = "2"
                                    Case "7"
                                        sDFEst = "3"
                                End Select

                                If fvalXmlProcessadoTransf(sChaveNfe, sCodTabEmitDest) Then

                                    If bAtivaLog Then
                                        fLog(arq.Name, "Função fvalXMLProcessadoTransf")
                                    End If

                                    Using con As SqlConnection = GetConnectionXML()  'Grava log
                                        Try
                                            con.Open()
                                            Dim cmd As New SqlCommand
                                            cmd.Connection = con
                                            cmd.CommandText = "INSERT INTO LOGEVENTOSXML (NOME_XML, DATAEMISSAO, SETOR, USUARIO, EVENTO, CRITICA) " +
                                                "VALUES ('" & arq.Name & "',convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), 'FIS', '" & sCodUsuario & "', 'I', 'XML TRANSFERIDO PARA A PASTA CRITICADOS, O MESMO JA FOI LANCADO NO SISTEMA.')"
                                            cmd.ExecuteReader()
                                        Catch ex As Exception
                                            Dim cmd As New SqlCommand
                                            cmd.Connection = con
                                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                            cmd.ExecuteReader()
                                            oReader.Close()
                                            con.Dispose()
                                            If bAtivaLog Then
                                                fLog(arq.Name, "Final do Processo 386")
                                            End If
                                            Continue For
                                        End Try
                                        con.Dispose()
                                    End Using
                                    oReader.Close()
                                    File.Delete(sCriticados & "\" & arq.Name)
                                    File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                    File.Delete(caminho & "\" & arq.Name)
                                    If bAtivaLog Then
                                        fLog(arq.Name, "Final do Processo 397")
                                    End If
                                    Continue For
                                End If

                                If fVerCadCliFor(e) Then
                                    If fLeItensNfe(caminho, arq, xmlDoc, e) Then
                                        PopulaTabelas(caminho, arq, xmlDoc, e)
                                        Continue For
                                    Else
                                        bEnviaEmail = True
                                    End If
                                Else
                                    bEnviaEmail = True
                                End If
                            Else
                                If bAtivaLog Then
                                    fLog(arq.Name, "Final do Processo 414")
                                End If
                                Continue For
                            End If
                        Else
                            bEnviaEmail = True
                        End If
                    Else
                        bEnviaEmail = True
                    End If
                Else
                    bEnviaEmail = True
                End If

                If bEnviaEmail Then
                    Dim bValEmailCad As Boolean = False
                    Using con As SqlConnection = GetConnectionXML() 'Atualiza Flag para E (enviado)
                        Try
                            con.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            Dim dr As SqlDataReader
                            cmd.CommandText = "SELECT DISTINCT (FLAG_STATUS) FROM CRITICAXML WHERE FLAG_STATUS IS NULL AND NOME_XML = '" & arq.Name & "' AND SETOR = 'CMP'"
                            dr = cmd.ExecuteReader()
                            dr.Read()
                            'elhernandes
                            If dr.HasRows Then
                                If bValEmailCad Then
                                    bValEmailCad = True
                                End If
                            End If
                        Catch ex As Exception
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            con.Dispose()
                            If bAtivaLog Then
                                fLog(arq.Name, "Final do Processo 450")
                            End If
                            Continue For
                        End Try
                        con.Dispose()
                    End Using

                    If bValEmailCad Then
                        EnviaEmailCad(arq.Name, e)
                        Using con As SqlConnection = GetConnectionXML() 'Atualiza Flag para E (enviado)
                            Try
                                con.Open()
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                Dim dr As SqlDataReader
                                cmd.CommandText = "UPDATE CRITICAXML SET FLAG_STATUS = 'E' WHERE FLAG_STATUS IS NULL AND NOME_XML = '" & arq.Name & "' AND SETOR = 'CMP'"
                                dr = cmd.ExecuteReader()
                            Catch ex As Exception
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                cmd.ExecuteReader()
                                oReader.Close()
                                con.Dispose()
                                If bAtivaLog Then
                                    fLog(arq.Name, "Final do Processo 475")
                                End If
                                Continue For
                            End Try
                            con.Dispose()
                        End Using
                    End If

                    Dim bValEmailFiscal As Boolean = False

                    Using con As SqlConnection = GetConnectionXML() 'Atualiza Flag para E (enviado)
                        Try
                            con.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            Dim dr As SqlDataReader
                            cmd.CommandText = "SELECT DISTINCT (FLAG_STATUS) FROM CRITICAXML WHERE FLAG_STATUS IS NULL AND NOME_XML = '" & arq.Name & "' AND SETOR = 'FIS'"
                            dr = cmd.ExecuteReader()
                            dr.Read()
                            If dr.HasRows Then
                                'alterar essa funcao - elhernandes
                                If bValEmailFiscal Then
                                    bValEmailFiscal = True
                                End If
                            End If
                        Catch ex As Exception
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            con.Dispose()
                            If bAtivaLog Then
                                fLog(arq.Name, "Final do Processo 505")
                            End If
                            Continue For
                        End Try
                        con.Dispose()
                    End Using

                    If bValEmailFiscal Then
                        EnviaEmailFiscal(arq.Name, e)
                        Using con As SqlConnection = GetConnectionXML() 'Atualiza Flag para E (enviado)
                            Try
                                con.Open()
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                Dim dr As SqlDataReader
                                cmd.CommandText = "UPDATE CRITICAXML SET FLAG_STATUS = 'E' WHERE FLAG_STATUS IS NULL AND NOME_XML = '" & arq.Name & "' AND SETOR = 'FIS'"
                                dr = cmd.ExecuteReader()
                            Catch ex As Exception
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                cmd.ExecuteReader()
                                oReader.Close()
                                con.Dispose()
                                If bAtivaLog Then
                                    fLog(arq.Name, "Final do Processo 530")
                                End If
                                Continue For
                            End Try
                            con.Dispose()
                        End Using
                    End If
                End If
            End If
            bValXml = True
            oReader.Close()

            If bAtivaLog Then
                fLog(arq.Name, "Fim do Processo!")
            End If

        Next

        If bAtivaLog Then
            fLog("Sem XML", "Pasta Vazia")
        End If

        If bAtivaEmail Then
            fEnviaEmalDiario(e)
        End If

        tempo.Enabled = True

    End Sub

    Protected Overrides Sub OnStop()
        fLog("Função:", "OnStop")
        tempo.Enabled = False
    End Sub

    Function fXmlTemp(ByVal caminho As String, ByVal arq As System.IO.FileInfo, ByVal xmlDoc As XmlDocument, ByVal e As System.EventArgs)
        fLog("Função:", "fXmlTemp")
        Dim ns As New XmlNamespaceManager(xmlDoc.NameTable)
        ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe")
        Dim xpathNav As XPathNavigator = xmlDoc.CreateNavigator()
        Dim node As XPathNavigator

        Dim sCnpjFor, sRazaoFor, sUFFor, sCnpjCli, sRazaoCli, sUFCli, sDataEmissao, sChaveNfe, sNF, sSerieNF, sPlaca, sUFPlaca, sEspecie, sDataSaiEnt, sModFrete, sCnpjTransp, sMarcaTransp, sNomeTransp,
            sIETransp, sEnderTransp, sMunTransp, sUFTransp, sNumDi, sDataDi, sLocalDesemb, sUFDesemb, sDataDesemb, sOrigem_CFOP, sCfop, sDescProd, sUnd, sCSTCofins, sCodMunicipio, sNroAdicao,
            sDNroItem, sCSTPis, sCSTIss, sNumPedido, sCstIcms, sCodProd, sOrigCST, sIcms, sinfCpl, sinfAdFisco, sQuery As String

        Dim dValTotNF, dValtotFrete, dValTotPrd, dValTotST, dValTotIpi, dValTotSeg, dValTotDesc, dValTotOutro, dQuantVol, dValTotIcms, dPesoLiq, dPesoBruto, dValProd, dValUnit, dQuant, dValFrete,
            dValDesc, dValOutro, dValSeg, dAliqIpi, dValIpi, dValBCCofins, dAlqCofins, dValCofins, dValBCPis, dAlqPis, dValPis, dValBCIss, dAlqIss, dValIss, dAliqIcms, dValIcms, dValBcIcms, bAliqRedBCIcms,
            bAliqRedBCSTIcms, dAliqMvaST, dValBcSTIcms, dValIcmsSt, dAliqIcmsSt As Double

        Dim iCRT, i As Integer

        Dim bSai, bVal As Boolean

        i = 0
        bSai = False
        bVal = True
        sQuery = ""

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:emit/nfe:CNPJ", ns)
        If Not node Is Nothing Then
            sCnpjFor = "'" & Strings.Left(node.InnerXml.ToString, 14) & "'" 'Cnpj do Emitente(Fornecedor) da NF

        Else
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:emit/nfe:CPF", ns)
            If Not node Is Nothing Then
                sCnpjFor = "'" & Strings.Left(node.InnerXml.ToString, 14) & "'" 'Cnpj do Emitente(Fornecedor) da NF
            Else
                sCnpjFor = "NULL"
            End If
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:emit/nfe:xNome", ns)
        If Not node Is Nothing Then
            sRazaoFor = "'" & Strings.Left(node.InnerXml.ToString, 60) & "'" 'Razao Social do Fornecedor
        Else
            sRazaoFor = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:emit/nfe:enderEmit/nfe:UF", ns)
        If Not node Is Nothing Then
            sUFFor = "'" & Strings.Left(node.InnerXml.ToString, 2) & "'" 'UF Fornecedor
        Else
            sUFFor = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:dest/nfe:CNPJ", ns)
        If Not node Is Nothing Then
            sCnpjCli = "'" & Strings.Left(node.InnerXml.ToString, 14) & "'" 'Cnpj do Destinatario(Cliente) da NF
        Else
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:dest/nfe:CPF", ns)
            If Not node Is Nothing Then
                sCnpjCli = "'" & Strings.Left(node.InnerXml.ToString, 14) & "'" 'Cnpj do Destinatario(Cliente) da NF
            Else
                sCnpjCli = "NULL"
            End If
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:dest/nfe:xNome", ns)
        If Not node Is Nothing Then
            sRazaoCli = "'" & Strings.Left(node.InnerXml.ToString, 60) & "'" 'Razao Social do Cliente
        Else
            sRazaoCli = "NULL"
        End If
        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:dest/nfe:enderDest/nfe:UF", ns)
        If Not node Is Nothing Then
            sUFCli = "'" & Strings.Left(node.InnerXml.ToString, 2) & "'" 'UF Cliente
        Else
            sUFCli = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:dEmi", ns)
        If Not node Is Nothing Then
            sDataEmissao = "'" & node.InnerXml.ToString & "'"
        Else
            sDataEmissao = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:emit/nfe:CRT", ns)
        If Not node Is Nothing Then
            iCRT = Strings.Left(node.InnerXml.ToString, 1)
        Else
            iCRT = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe", ns)
        If Not node Is Nothing Then
            sChaveNfe = "'" & Strings.Right(node.GetAttribute("Id", ""), 44) & "'"       'Chave de acesso da NF-e
        Else
            sChaveNfe = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[1]/nfe:prod/nfe:CFOP", ns)
        If Not node Is Nothing Then
            sOrigem_CFOP = "'" & Strings.Left(node.InnerXml.ToString, 1) & "'"
        Else
            sOrigem_CFOP = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:nNF", ns)
        If Not node Is Nothing Then
            sNF = "'" & Strings.Right(Space(9).Replace(" ", "0") & node.InnerXml.ToString, 9) & "'" 'Numero da NF
        Else
            sNF = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:serie", ns)
        If Not node Is Nothing Then
            sSerieNF = "'" & Strings.Left(node.InnerXml.ToString, 3) & "'" 'Numero de Serie
        Else
            sSerieNF = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vNF", ns)
        If Not node Is Nothing Then
            dValTotNF = node.InnerXml.ToString.Replace(".", ",")
        Else
            dValTotNF = 0
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vFrete", ns)
        If Not node Is Nothing Then
            dValtotFrete = node.InnerXml.ToString.Replace(".", ",")
        Else
            dValtotFrete = 0
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vProd", ns)
        If Not node Is Nothing Then
            dValTotPrd = node.InnerXml.ToString.Replace(".", ",")
        Else
            dValTotPrd = 0
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vSeg", ns)
        If Not node Is Nothing Then
            dValTotSeg = node.InnerXml.ToString.Replace(".", ",")
        Else
            dValTotSeg = 0
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vDesc", ns)
        If Not node Is Nothing Then
            dValTotDesc = node.InnerXml.ToString.Replace(".", ",")
        Else
            dValTotDesc = 0
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vOutro", ns)
        If Not node Is Nothing Then
            dValTotOutro = node.InnerXml.ToString.Replace(".", ",")
        Else
            dValTotOutro = 0
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vST", ns)
        If Not node Is Nothing Then
            dValTotST = node.InnerXml.ToString.Replace(".", ",")
        Else
            dValTotST = 0
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vIPI", ns)
        If Not node Is Nothing Then
            dValTotIpi = node.InnerXml.ToString.Replace(".", ",")
        Else
            dValTotIpi = 0
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:veicTransp/nfe:placa", ns)
        If Not node Is Nothing Then
            sPlaca = "'" & Strings.Left(node.InnerXml.ToString, 8) & "'"   'Placa Transportadora
        Else
            sPlaca = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:veicTransp/nfe:UF", ns)
        If Not node Is Nothing Then
            sUFPlaca = "'" & Strings.Left(node.InnerXml.ToString, 2) & "'"   'UF Placa Transportadora
        Else
            sUFPlaca = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:vol/nfe:pesoL", ns)
        If Not node Is Nothing Then
            dPesoLiq = node.InnerXml.ToString.Replace(".", ",")    'Peso liquido
        Else
            dPesoLiq = 0
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:vol/nfe:pesoB", ns)
        If Not node Is Nothing Then
            dPesoBruto = node.InnerXml.ToString.Replace(".", ",")     'Peso bruto             
        Else
            dPesoBruto = 0
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:vol/nfe:esp", ns)
        If Not node Is Nothing Then
            sEspecie = "'" & Strings.Left(node.InnerXml.ToString, 60) & "'"  'Especie
        Else
            sEspecie = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:dhSaiEnt", ns)
        If Not node Is Nothing Then
            Dim dtSai As String = node.InnerXml.ToString

            sDataSaiEnt = "'" & dtSai.Substring(0,19) & "'"      'Data Entr/Saida

        Else
            sDataSaiEnt = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:modFrete", ns)
        If Not node Is Nothing Then
            sModFrete = "'" & Strings.Left(node.InnerXml.ToString, 1) & "'"
        Else
            sModFrete = "'9'"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:transporta/nfe:CNPJ", ns)
        If Not node Is Nothing Then
            sCnpjTransp = "'" & Strings.Left(node.InnerXml.ToString, 14) & "'"
        Else
            sCnpjTransp = "NULL"
        End If
        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:vol/nfe:qVol", ns)
        If Not node Is Nothing Then
            dQuantVol = node.InnerXml.ToString.Replace(".", ",")
        Else
            dQuantVol = 0
        End If
        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:vol/nfe:marca", ns)
        If Not node Is Nothing Then
            sMarcaTransp = "'" & Strings.Left(node.InnerXml.ToString, 10) & "'"
        Else
            sMarcaTransp = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:transporta/nfe:xNome", ns)
        If Not node Is Nothing Then
            sNomeTransp = "'" & Strings.Left(node.InnerXml.ToString, 60) & "'"
        Else
            sNomeTransp = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:transporta/nfe:IE", ns)
        If Not node Is Nothing Then
            sIETransp = "'" & node.InnerXml.ToString & "'"
        Else
            sIETransp = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:transporta/nfe:xEnder", ns)
        If Not node Is Nothing Then
            sEnderTransp = "'" & Strings.Left(node.InnerXml.ToString, 60) & "'"
        Else
            sEnderTransp = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:transporta/nfe:xMun", ns)
        If Not node Is Nothing Then
            sMunTransp = "'" & Strings.Left(node.InnerXml.ToString, 60) & "'"
        Else
            sMunTransp = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:transporta/nfe:UF", ns)
        If Not node Is Nothing Then
            sUFTransp = "'" & Strings.Left(node.InnerXml.ToString, 2) & "'"
        Else
            sUFTransp = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vICMS", ns)
        If Not node Is Nothing Then
            dValTotIcms = node.InnerXml.ToString.Replace(".", ",")
        Else
            dValTotIcms = 0
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[1]/nfe:prod/nfe:DI/nfe:nDI", ns)
        If Not node Is Nothing Then
            sNumDi = "'" & Strings.Left(node.InnerXml.ToString, 12) & "'"
        Else
            sNumDi = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[1]/nfe:prod/nfe:DI/nfe:dDI", ns)
        If Not node Is Nothing Then
            sDataDi = "'" & node.InnerXml.ToString & "'"
        Else
            sDataDi = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[1]/nfe:prod/nfe:DI/nfe:xLocDesemb", ns)
        If Not node Is Nothing Then
            sLocalDesemb = "'" & Strings.Left(node.InnerXml.ToString, 60) & "'"
        Else
            sLocalDesemb = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[1]/nfe:prod/nfe:DI/nfe:UFDesemb", ns)
        If Not node Is Nothing Then
            sUFDesemb = "'" & Strings.Left(node.InnerXml.ToString, 2) & "'"
        Else
            sUFDesemb = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[1]/nfe:prod/nfe:DI/nfe:dDesemb", ns)
        If Not node Is Nothing Then
            sDataDesemb = "'" & node.InnerXml.ToString & "'"
        Else
            sDataDesemb = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:infAdic/nfe:infCpl", ns)
        If Not node Is Nothing Then
            sinfCpl = "'" & Strings.Left(node.InnerXml.ToString, 5000) & "'"
        Else
            sinfCpl = "NULL"
        End If

        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:infAdic/nfe:infAdFisco", ns)
        If Not node Is Nothing Then
            sinfAdFisco = "'" & Strings.Left(node.InnerXml.ToString, 2000) & "'"
        Else
            sinfAdFisco = "NULL"
        End If

        sQuery = sQuery + "DECLARE @ERROR INT SET @ERROR = 0 BEGIN TRANSACTION  BEGIN TRY INSERT INTO XMLTEMPCABEC (CNPJ_CPF_FOR, RAZAO_FOR, EST_FOR, CNPJ_CPF_CLI, RAZAO_CLI, EST_CLI, DT_EMISSAO, CRT, CHV_NFE, " +
            "ORIGEM_CFOP, NUM_NF, SERIE_NF, VAL_TOT_NF, VAL_TOT_FRETE, VAL_TOT_PRD, VAL_TOT_SEG, VAL_TOT_DESC, VAL_TOT_OUTRO, VAL_TOT_ST, VAL_TOT_IPI, PLACA, UF_PLACA, PESO_LIQ, PESO_BRUTO, ESPECIE, DT_SAIENT, " +
            "MOD_FRETE, CNPJ_TRANSP, QTD_VOLUME, MARCA_TRANSP, NOME_TRANSP, IE_TRANSP, END_TRANSP, MUNIC_TRANSP, UF_TRANSP, VAL_TOT_ICMS, NUM_DI, DT_DI, LOCAL_DESEMB, UF_DESEMB, DT_DESEMB, INF_CPL, INF_AD_FISCO) " +
            "VALUES (" & sCnpjFor & ", " & sRazaoFor & ", " & sUFFor & ", " & sCnpjCli & ", " & sRazaoCli & ", " & sUFCli & ", convert(datetime, " & sDataEmissao & ", 121), " & iCRT & ", " & sChaveNfe & ", " +
            "" & sOrigem_CFOP & ", " & sNF & ", " & sSerieNF & ", " & Str(dValTotNF) & ", " & Str(dValtotFrete) & ", " & Str(dValTotPrd) & ", " & Str(dValTotSeg) & ", " & Str(dValTotDesc) & ", " +
            "" & Str(dValTotOutro) & ", " & Str(dValTotST) & ", " & Str(dValTotIpi) & ", " & sPlaca & ", " & sUFPlaca & ", " & Str(dPesoLiq) & ", " & Str(dPesoBruto) & ", " & sEspecie & ", " +
            "convert(datetime, " & sDataSaiEnt & ", 121), " & sModFrete & ", " & sCnpjTransp & ", " & Str(dQuantVol) & ", " & sMarcaTransp & ", " & sNomeTransp & ", " & sIETransp & ", " & sEnderTransp & ", " +
            "" & sMunTransp & ", " & sUFTransp & ", " & Str(dValTotIcms) & ", " & sNumDi & ", convert(datetime, " & sDataDi & ", 121), " & sLocalDesemb & ", " & sUFDesemb & ", " +
            "convert(datetime, " & sDataDesemb & ", 121), " & sinfCpl & ", " & sinfAdFisco & ") " +
            "END TRY BEGIN CATCH SET @ERROR = 1 INSERT INTO ERROSDBXML (NOME_XML, DATA, ERRO) VALUES " +
            "('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), (SELECT ERROR_MESSAGE())) END CATCH "

        bSai = False
        i = 0
        'Inicia o processamento dos produtos #produtos
        While Not bSai
            i = i + 1
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:xProd", ns)
            If node Is Nothing Then
                bSai = True
                i = i - 1
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:CFOP", ns)
                If Not node Is Nothing Then
                    sCfop = node.InnerXml.Substring(0, 4)
                Else
                    sCfop = "NULL"
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:cProd", ns)
                If Not node Is Nothing Then
                    sCodProd = "'" & Strings.Left(node.InnerXml.ToString, 60) & "'"
                Else
                    sCodProd = "NULL"
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:NCM", ns)
                If Not node Is Nothing Then
                    prodNCM = "'" & Strings.Left(node.InnerXml.ToString, 60) & "'"
                Else
                    prodNCM = "NULL"
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:xProd", ns)
                If Not node Is Nothing Then
                    sDescProd = "'" & Strings.Left(node.InnerXml.ToString, 120) & "'"
                Else
                    sDescProd = "NULL"
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:qCom", ns)
                If Not node Is Nothing Then
                    dQuant = node.InnerXml.ToString.Replace(".", ",")      'Quantidade
                Else
                    dQuant = 0
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:uCom", ns)
                If Not node Is Nothing Then
                    sUnd = "'" & Strings.Left(node.InnerXml.ToString, 6) & "'"
                Else
                    sUnd = ""
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vProd", ns)
                If Not node Is Nothing Then
                    dValProd = node.InnerXml.ToString.Replace(".", ",")
                Else
                    dValProd = 0
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vUnCom", ns)
                If Not node Is Nothing Then
                    dValUnit = node.InnerXml.ToString.Replace(".", ",")     'Preco unitario
                Else
                    dValUnit = 0
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vFrete", ns)
                If Not node Is Nothing Then
                    dValFrete = node.InnerXml.ToString.Replace(".", ",")
                Else
                    dValFrete = 0
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vDesc", ns)
                If Not node Is Nothing Then
                    dValDesc = node.InnerXml.ToString.Replace(".", ",")
                Else
                    dValDesc = 0
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vOutro", ns)
                If Not node Is Nothing Then
                    dValOutro = node.InnerXml.ToString.Replace(".", ",")
                Else
                    dValOutro = 0
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vSeg", ns)
                If Not node Is Nothing Then
                    dValSeg = node.InnerXml.ToString.Replace(".", ",")
                Else
                    dValSeg = 0
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:IPI/nfe:IPITrib/nfe:pIPI", ns)
                If Not node Is Nothing Then
                    dAliqIpi = node.InnerXml.ToString.Replace(".", ",")
                Else
                    dAliqIpi = 0
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:IPI/nfe:IPITrib/nfe:vIPI", ns)
                If Not node Is Nothing Then
                    dValIpi = node.InnerXml.ToString.Replace(".", ",")
                Else
                    dValIpi = 0
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:CST", ns)
                If Not node Is Nothing Then
                    sCSTCofins = "'" & Strings.Left(node.InnerXml.ToString, 3) & "'"

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:vBC", ns)
                    If Not node Is Nothing Then
                        dValBCCofins = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValBCCofins = 0
                    End If
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:pCOFINS", ns)
                    If Not node Is Nothing Then
                        dAlqCofins = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dAlqCofins = 0
                    End If
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:vCOFINS", ns)
                    If Not node Is Nothing Then
                        dValCofins = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValCofins = 0
                    End If

                Else
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:COFINS/nfe:COFINSNT/nfe:CST", ns)
                    If Not node Is Nothing Then
                        sCSTCofins = "'" & Strings.Left(node.InnerXml.ToString, 3) & "'"
                        dValBCCofins = 0
                        dAlqCofins = 0
                        dValCofins = 0
                    Else
                        sCSTCofins = "NULL"
                        dValBCCofins = 0
                        dAlqCofins = 0
                        dValCofins = 0
                    End If
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ISSQN/nfe:cMunFG", ns)
                If Not node Is Nothing Then
                    sCodMunicipio = "'" & Strings.Left(node.InnerXml.ToString, 7) & "'"
                Else
                    sCodMunicipio = "NULL"
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:DI/nfe:adi/nfe:nAdicao", ns)
                If Not node Is Nothing Then
                    sNroAdicao = "'" & Strings.Left(node.InnerXml.ToString, 9) & "'"
                Else
                    sNroAdicao = "NULL"
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:DI/nfe:adi/nfe:nSeqAdic", ns)
                If Not node Is Nothing Then
                    sDNroItem = "'" & Strings.Left(node.InnerXml.ToString, 9) & "'"
                Else
                    sDNroItem = "NULL"
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:PIS/nfe:PISAliq/nfe:CST", ns)
                If Not node Is Nothing Then
                    sCSTPis = "'" & Strings.Left(node.InnerXml.ToString, 3) & "'"

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:PIS/nfe:PISAliq/nfe:vBC", ns)
                    If Not node Is Nothing Then
                        dValBCPis = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValBCPis = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:PIS/nfe:PISAliq/nfe:pPIS", ns)
                    If Not node Is Nothing Then
                        dAlqPis = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dAlqPis = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:PIS/nfe:PISAliq/nfe:vPIS", ns)
                    If Not node Is Nothing Then
                        dValPis = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValPis = 0
                    End If
                Else
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:PIS/nfe:PISNT/nfe:CST", ns)
                    If Not node Is Nothing Then
                        sCSTPis = "'" & Strings.Left(node.InnerXml.ToString, 3) & "'"
                        dValBCPis = 0
                        dAlqPis = 0
                        dValPis = 0
                    Else
                        sCSTPis = "NULL"
                        dValBCPis = 0
                        dAlqPis = 0
                        dValPis = 0
                    End If
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ISSQN/nfe:cSitTrib", ns)
                If Not node Is Nothing Then
                    sCSTIss = "'" & Strings.Left(node.InnerXml.ToString, 3) & "'"

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ISSQN/nfe:vBC", ns)
                    If Not node Is Nothing Then
                        dValBCIss = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValBCIss = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ISSQN/nfe:vAliq", ns)
                    If Not node Is Nothing Then
                        dAlqIss = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dAlqIss = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ISSQN/nfe:vISSQN", ns)
                    If Not node Is Nothing Then
                        dValIss = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValIss = 0
                    End If
                Else
                    sCSTIss = "NULL"
                    dValBCIss = 0
                    dAlqIss = 0
                    dValIss = 0
                End If

                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:xPed", ns)
                If Not node Is Nothing Then
                    sNumPedido = "'" & Strings.Right(Space(10).Replace(" ", "0") & node.InnerXml.ToString, 60) & "'"
                Else
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:compra/nfe:xPed", ns)
                    If Not node Is Nothing Then
                        sNumPedido = "'" & Strings.Right(Space(10).Replace(" ", "0") & node.InnerXml.ToString, 60) & "'"
                    Else
                        sNumPedido = "NULL"
                    End If
                End If

                If iCRT = 1 Then
                    sIcms = FPegaIcmsSn(xmlDoc, 1)
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:CSOSN", ns)
                    If Not node Is Nothing Then
                        sCstIcms = "'" & Strings.Left(node.InnerXml.ToString, 3) & "'"
                    Else
                        sCstIcms = "NULL"
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pCredSN", ns)
                    If Not node Is Nothing Then
                        dAliqIcms = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dAliqIcms = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:vCredICMSSN", ns)
                    If Not node Is Nothing Then
                        dValIcms = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValIcms = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:vBC", ns)
                    If Not node Is Nothing Then
                        dValBcIcms = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValBcIcms = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pRedBC", ns)
                    If Not node Is Nothing Then
                        bAliqRedBCIcms = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        bAliqRedBCIcms = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pRedBCST", ns)
                    If Not node Is Nothing Then
                        bAliqRedBCSTIcms = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        bAliqRedBCSTIcms = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pMVAST", ns)
                    If Not node Is Nothing Then
                        dAliqMvaST = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dAliqMvaST = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:vBCST", ns)
                    If Not node Is Nothing Then
                        dValBcSTIcms = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValBcSTIcms = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:vCredICMSSN", ns)
                    If Not node Is Nothing Then
                        dValIcmsSt = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValIcmsSt = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pICMSST", ns)
                    If Not node Is Nothing Then
                        dAliqIcmsSt = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dAliqIcmsSt = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:orig", ns)
                    If Not node Is Nothing Then
                        sOrigCST = "'" & Strings.Left(node.InnerXml.ToString, 1) & "'"
                    Else
                        sOrigCST = "NULL"
                    End If

                Else
                    sIcms = FPegaIcms(xmlDoc, 1)
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:CST", ns)
                    If Not node Is Nothing Then
                        sCstIcms = "'" & Strings.Left(node.InnerXml.ToString, 3) & "'"
                    Else
                        sCstIcms = "NULL"
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pICMS", ns)
                    If Not node Is Nothing Then
                        dAliqIcms = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dAliqIcms = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:vICMS", ns)
                    If Not node Is Nothing Then
                        dValIcms = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValIcms = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:vBC", ns)
                    If Not node Is Nothing Then
                        dValBcIcms = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValBcIcms = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pRedBC", ns)
                    If Not node Is Nothing Then
                        bAliqRedBCIcms = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        bAliqRedBCIcms = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pRedBCST", ns)
                    If Not node Is Nothing Then
                        bAliqRedBCSTIcms = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        bAliqRedBCSTIcms = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pMVAST", ns)
                    If Not node Is Nothing Then
                        dAliqMvaST = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dAliqMvaST = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:vBCST", ns)
                    If Not node Is Nothing Then
                        dValBcSTIcms = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValBcSTIcms = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:vICMSST", ns)
                    If Not node Is Nothing Then
                        dValIcmsSt = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dValIcmsSt = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pICMSST", ns)
                    If Not node Is Nothing Then
                        dAliqIcmsSt = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dAliqIcmsSt = 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:orig", ns)
                    If Not node Is Nothing Then
                        sOrigCST = "'" & Strings.Left(node.InnerXml.ToString, 1) & "'"
                    Else
                        sOrigCST = "NULL"
                    End If
                End If

                sQuery = sQuery + "BEGIN TRY INSERT INTO XMLTEMPITENS (CHV_NFE, ITEM, CFOP, COD_PRD_FOR, DESCRIC, QTD, UND_MED, VAL_PRD, VAL_UNIT_PRD, VAL_FRETE, VAL_DESC, VAL_OUTROS, VAL_SEG, ALIQ_IPI, VAL_IPI, CST_COFINS, " +
                    "VAL_BC_COFINS, ALIQ_COFINS, VAL_COFINS, COD_MUNICIPIO, NUM_ADICAO, NUM_DI_ITEM, CST_PIS, VAL_BC_PIS, ALIQ_PIS, VAL_PIS, CST_ISS, VAL_BC_ISS, ALIQ_ISS, VAL_ISS, NUM_PED, CST_ICMS, ALIQ_ICMS, " +
                    "VAL_ICMS, VAL_BC_ICMS, ALIQ_RED_BC_ICMS, ALIQ_RED_BCST_ICMS, ALIQ_MVAST_ICMS, VAL_BCST_ICMS, VAL_ICMSST, ALIQ_ICMSST, ORIG_CST) VALUES (" +
                    "" & sChaveNfe & ", " & i & ", " & sCfop & ", " & sCodProd & ", " & sDescProd & ", " & Str(dQuant) & ", " & sUnd & ", " & Str(dValProd) & ", " & Str(dValUnit) & ", " & Str(dValFrete) & ", " +
                    "" & Str(dValDesc) & ", " & Str(dValOutro) & ", " & Str(dValSeg) & ", " & Str(dAliqIpi) & ", " & Str(dValIpi) & ", " & sCSTCofins & ", " & Str(dValBCCofins) & ", " & Str(dAlqCofins) & ", " +
                    "" & Str(dValCofins) & ", " & sCodMunicipio & ", " & sNroAdicao & ", " & sDNroItem & ", " & sCSTPis & ", " & Str(dValBCPis) & ", " & Str(dAlqPis) & ", " & Str(dValPis) & ", " & sCSTIss & ", " +
                    "" & Str(dValBCIss) & ", " & Str(dAlqIss) & ", " & Str(dValIss) & ", " & sNumPedido & ", " & sCstIcms & ", " & Str(dAliqIcms) & ", " & Str(dValIcms) & ", " & Str(dValBcIcms) & ", " & Str(bAliqRedBCIcms) & ", " +
                    "" & Str(bAliqRedBCSTIcms) & ", " & Str(dAliqMvaST) & ", " & Str(dValBcSTIcms) & ", " & Str(dValIcmsSt) & ", " & Str(dAliqIcmsSt) & ", " & sOrigCST & ") " +
                    "END TRY BEGIN CATCH SET @ERROR = 1 INSERT INTO ERROSDBXML (NOME_XML, DATA, ERRO) VALUES " +
                    "('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), (SELECT ERROR_MESSAGE())) END CATCH "
            End If

        End While

        sQuery = sQuery + "IF @ERROR != 0 BEGIN ROLLBACK TRANSACTION RETURN END ELSE BEGIN COMMIT TRANSACTION END "

        Using con As SqlConnection = GetConnectionXML()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                Dim dr As SqlDataReader
                cmd.CommandText = sQuery
                dr = cmd.ExecuteReader()
            Catch ex As Exception

                Dim sSource As String
                Dim sLog As String
                Dim sEvent As String

                sSource = "InspetorXML"
                sLog = "Application"
                sEvent = "Erro na gravação das tabelas temporarias (XMLTEMPCABEC e XMLTEMPITENS). Log: " + ex.Message

                If Not EventLog.SourceExists(sSource) Then
                    EventLog.CreateEventSource(sSource, sLog)
                End If

                Dim ELog As New EventLog(sLog)
                ELog.Source = sSource
                ELog.WriteEntry(sEvent)
                ELog.WriteEntry(sEvent, EventLogEntryType.Error, 1, CType(3, Short))

                bVal = False

            End Try
            con.Dispose()
        End Using

        Return bVal
    End Function

    Function FValidaTipoNf(ByVal caminho As String, ByVal arq As System.IO.FileInfo, ByVal xmlDoc As XmlDocument, ByVal e As System.EventArgs)
        fLog("FValidaTipoNf", "Função:")
        Dim ns As New XmlNamespaceManager(xmlDoc.NameTable)
        ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe")
        Dim xpathNav As XPathNavigator = xmlDoc.CreateNavigator()
        Dim node As XPathNavigator
        'Define as variaveis para recebimento do XML
        Dim bValNfEmit As Boolean = False
        Dim bValNfDest As Boolean = False

        'Copia sigamat.emp
        'File.Delete(sSigamatDest & "\sigamat.dbf")
        'File.Copy(sSigamatOrig & "\sigamat.emp", sSigamatDest & "\sigamat.dbf")

        'Recebe o conteudo do XML nas variaveis
        'Using con As OdbcConnection = GetConnectionDBF()
        '    Try
        '        con.Open()
        '        Dim oCmd As OdbcCommand = con.CreateCommand()
        '        oCmd.CommandText = "SELECT * FROM " & sSigamatDest & "\sigamat.dbf WHERE M0_CGC = '" & sCnpjEmit & "'"
        '        Dim dt As New DataTable()
        '        dt.Load(oCmd.ExecuteReader())
        '        If Not dt.Rows.Count = 0 Then
        '            sCodTabEmit = dt.Rows.Item(0)("M0_CODIGO").ToString
        '            sFilialEmit = dt.Rows.Item(0)("M0_CODFIL").ToString
        '            bValNfEmit = True
        '        Else
        '            sCodTabEmit = ""
        '            sFilialEmit = ""
        '        End If
        '    Catch ex As Exception
        '        con.Dispose()
        '    End Try
        '    con.Dispose()
        'End Using

        Using con As SqlConnection = GetConnectionXML()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT M0_CODIGO, M0_CODFIL FROM SIGAMAT WHERE M0_CGC = '" & sCnpjEmit & "'"
                'cmd.CommandText = "SELECT M0_CODIGO, M0_CODFIL FROM SIGAMAT WHERE M0_CGC = '" & sCnpjDest & "'"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                dr.Read()
                If dr.HasRows Then
                    sCodTabEmit = dr.Item(0).ToString
                    sFilialEmit = dr.Item(1).ToString
                    bValNfEmit = True
                Else
                    sCodTabEmit = ""
                    sFilialEmit = ""
                End If
            Catch ex As Exception
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                oReader.Close()
                con.Dispose()
                OnElapsedTime(Me, e)
            End Try
            con.Dispose()
        End Using

        'Using con As OdbcConnection = GetConnectionDBF()
        '    Try
        '        con.Open()
        '        Dim oCmd As OdbcCommand = con.CreateCommand()
        '        oCmd.CommandText = "SELECT * FROM " & sSigamatDest & "\sigamat.dbf WHERE M0_CGC = '" & sCnpjDest & "'"
        '        Dim dt As New DataTable()
        '        dt.Load(oCmd.ExecuteReader())
        '        If Not dt.Rows.Count = 0 Then
        '            sCodTabDest = dt.Rows.Item(0)("M0_CODIGO").ToString
        '            sFilialDest = dt.Rows.Item(0)("M0_CODFIL").ToString
        '            bValNfDest = True
        '        Else
        '            sCodTabDest = ""
        '            sFilialDest = ""
        '        End If
        '    Catch ex As Exception
        '        con.Dispose()
        '    End Try
        '    con.Dispose()
        'End Using
        Using con As SqlConnection = GetConnectionXML()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT M0_CODIGO, M0_CODFIL FROM SIGAMAT WHERE M0_CGC = '" & sCnpjDest & "'"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                dr.Read()
                If dr.HasRows Then
                    sCodTabDest = dr.Item(0).ToString
                    sFilialDest = dr.Item(1).ToString
                    bValNfDest = True
                Else
                    sCodTabDest = ""
                    sFilialDest = ""
                End If
            Catch ex As Exception
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                oReader.Close()
                con.Dispose()
                OnElapsedTime(Me, e)
            End Try
            con.Dispose()
        End Using
        If Not sCodTabEmit = "" Then
            sCodTabEmitDest = sCodTabEmit
        ElseIf Not sCodTabDest = "" Then
            sCodTabEmitDest = sCodTabDest
        End If
        If sCodTabEmitDest = "" Then
            sTpNf = "N/A"
        Else
            Using con As SqlConnection = GetConnectionERP()
                Try
                    con.Open()	
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT A2_COD, A2_LOJA FROM SA2" & sCodTabEmitDest & "0 WHERE A2_CGC = '" & sCnpjEmit & "'"
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        sCodEmpEmit = dr.Item(0).ToString.Replace(" ", "")
                        sLojaEmit = dr.Item(1).ToString
                    Else
                        sCodEmpEmit = ""
                        sLojaEmit = ""
                    End If
                Catch ex As Exception
                    Using conError As SqlConnection = GetConnectionXML()
                        conError.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = conError
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        conError.Dispose()
                        OnElapsedTime(Me, e)
                    End Using
                End Try
                con.Dispose()
            End Using
            Using con As SqlConnection = GetConnectionERP()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    Dim dr As SqlDataReader
                    cmd.CommandText = "SELECT A1_COD, A1_LOJA, A1_TIPO FROM SA1" & sCodTabEmitDest & "0 WHERE A1_CGC = '" & sCnpjDest & "'"
                    dr = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        sCodEmpDest = dr.Item(0).ToString.Replace(" ", "")
                        sLojaDest = dr.Item(1).ToString
                        sTipoCli = dr.Item(2).ToString
                    Else
                        sCodEmpDest = ""
                        sLojaDest = ""
                        sTipoCli = ""
                    End If
                Catch ex As Exception
                    Using conError As SqlConnection = GetConnectionXML()
                        conError.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = conError
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        conError.Dispose()
                        OnElapsedTime(Me, e)
                    End Using
                End Try
                con.Dispose()
            End Using

            If bValNfEmit And bValNfDest Then
                Dim iProcessaSaida As Integer = 0
                Using con As SqlConnection = GetConnectionXML() 'VERIFICA SE IRÁ PROCESSAR SAIDAS
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT ATIVO FROM REGRASXML WHERE TIPO_VALIDACAO = 'SAIDA' AND ORDEM = 1 AND TIPO = 'S' AND PROCESSO = 'SAIDA'"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            iProcessaSaida = dr.Item(0) * -1
                        End If
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using
                If iProcessaSaida = 1 Then
                    sTpNf = "TRANSF_ENTSAI"
                Else
                    sTpNf = "TRANSF_ENT"
                End If
            ElseIf bValNfEmit Then
                If sCnpjDest = "" Then
                    sTpNf = "ENT_IMP"
                Else
                    sTpNf = "SAIDA"
                End If
            ElseIf bValNfDest Then
                sTpNf = "ENTRADA"
            Else
                sTpNf = "N/A"
            End If
        End If
        Return sTpNf
    End Function

    Sub PopulaTabelas(ByVal caminho As String, arq As System.IO.FileInfo, ByVal xmlDoc As XmlDocument, ByVal e As System.EventArgs)
        fLog("Função:", "PopulaTabelas")
        If bAtivaLog Then
            fLog(arq.Name, "Função PopulaTabelas")
        End If

        Try
            tempo.Enabled = False

            GeraCabNf(caminho, xmlDoc, e)
            'Cria uma instância de um documento XML
            Dim ns As New XmlNamespaceManager(xmlDoc.NameTable)
            ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe")
            Dim xpathNav As XPathNavigator = xmlDoc.CreateNavigator()
            Dim node As XPathNavigator
            Dim bSai As Boolean = False
            Dim i As Integer = 0
            While Not bSai
                i = i + 1
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:xProd", ns)
                If node Is Nothing Then
                    bSai = True
                    i = i - 1
                    Exit While
                Else
                    GeraItensNf(caminho, xmlDoc, i, e)
                End If
            End While
            If Not sTpNf = "TRANSF_ENTSAI" Then
                oReader.Close()
                File.Delete(sProcessado & "\" & arq.Name)
                File.Copy(caminho & "\" & arq.Name, sProcessado & "\" & arq.Name)
                File.Delete(caminho & "\" & arq.Name)
            End If
            Using con As SqlConnection = GetConnectionXML()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    Dim dr As SqlDataReader
                    cmd.CommandText = "DELETE FROM CRITICAXML WHERE NOME_XML = '" & arq.Name & "'"
                    dr = cmd.ExecuteReader()
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con.Dispose()
            End Using
        Catch ex As Exception
            Using con As SqlConnection = GetConnectionXML()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                oReader.Close()
                con.Dispose()
                OnElapsedTime(Me, e)
            End Using
        Finally
            'tempo.Enabled = True
        End Try
    End Sub

    Function fLeItensNfe(ByVal caminho As String, ByVal arq As System.IO.FileInfo, ByVal xmlDoc As XmlDocument, ByVal e As System.EventArgs)
        fLog("Função", "fLeItensNfe")
        If bAtivaLog Then
            fLog(arq.Name, "Função fLeItensNfe")
        End If

        Dim ns As New XmlNamespaceManager(xmlDoc.NameTable)
        ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe")
        Dim xpathNav As XPathNavigator = xmlDoc.CreateNavigator()
        Dim node As XPathNavigator
        Dim i As Integer = 0
        Dim iItens As Integer
        Dim bSai As Boolean = False
        Dim sCodProd, sIdPrd As String
        Dim sNomeProd As String
        Dim sUnd As String
        Dim bVal As Boolean = True
        Dim bValCadTrib As Boolean = True
        Dim bProdCad As Boolean = True
        Dim bProdXFor As Boolean = True
        Dim bValGrpTrib As Boolean = True
        Dim bValTemRegra As Boolean = False
        Dim dAliqIcms, dVIcms, dBc, dIpi, dValIpi, bRedBC, bRedBCST, dValProd, dValFrete, dValDesc, dValOutro, dValSeg, dMvaST, dVIcmsSt, dBcST, dAliqIcmsSt As Double
        Dim sIcms, sCst As String
        Dim bValXmlIg As Boolean = True
        Dim dValTNFiscal, dValTNf, dValTPrd, dValTICMSST, dValTFrete, dValTSeguro, dValTDesc, dValTOutraDesp, dValTIPI, dValTIcms As Double
        Dim dValTNfCalc, dValTICMSSTCalc As Double


        'Variaveis para conectar ao banco
        Dim usuario, senha, banco, servidor As String
        Dim configurationAppSettings As System.Configuration.AppSettingsReader = New System.Configuration.AppSettingsReader()
        usuario = configurationAppSettings.GetValue("userBD", GetType(System.String))
        senha = configurationAppSettings.GetValue("passBD", GetType(System.String))
        banco = configurationAppSettings.GetValue("DBXML", GetType(System.String))
        servidor = configurationAppSettings.GetValue("Server", GetType(System.String))
        Dim dbInspetor As DB = New DB(servidor, banco, usuario, senha)


        Dim flagStatus As ArrayList = dbInspetor.consulta("SELECT DISTINCT (FLAG_STATUS) AS FLAG_STATUS FROM CRITICAXML WHERE FLAG_STATUS = 'I' AND NOME_XML = '" & arq.Name & "'", "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & "')")

        If flagStatus.Count > 0 Then
            bValXmlIg = False
        Else
            OnElapsedTime(Me, e)
        End If
        'Using con As SqlConnection = GetConnectionXML()
        '    Try
        '        con.Open()
        '        Dim cmd As New SqlCommand
        '        cmd.Connection = con
        '        Dim dr As SqlDataReader
        '        cmd.CommandText = "SELECT DISTINCT (FLAG_STATUS) FROM CRITICAXML WHERE FLAG_STATUS = 'I' AND NOME_XML = '" & arq.Name & "'"
        '        dr = cmd.ExecuteReader()
        '        If dr.HasRows Then
        '            bValXmlIg = False
        '        End If
        '    Catch ex As Exception
        '        Dim cmd As New SqlCommand
        '        cmd.Connection = con
        '        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
        '        cmd.ExecuteReader()
        '        oReader.Close()
        '        con.Dispose()
        '        OnElapsedTime(Me, e)
        '    End Try
        '    con.Dispose()
        'End Using
        Dim bValCadProd As Boolean = False
        Dim bValArq As Boolean = False
        If bValXmlIg Then
            Dim bValEnviado As Boolean = True
            Using con As SqlConnection = GetConnectionXML()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    Dim dr As SqlDataReader
                    cmd.CommandText = "SELECT DISTINCT (FLAG_STATUS) FROM CRITICAXML WHERE FLAG_STATUS = 'E' AND NOME_XML = '" & arq.Name & "'"
                    dr = cmd.ExecuteReader()
                    If dr.HasRows Then
                        bValEnviado = False
                        bVal = False
                    End If
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con.Dispose()
            End Using
            Dim bValServico As Boolean = True
            Dim sCompCfop As String
            Dim iItemServ As Integer = 1
            Dim sCodXDesc(1, 0) As String

            Dim sIcmsSnxLr As String = ""
            Dim sIcmsTemp As String = ""

            Dim bValMvaSt As Boolean = False
            Dim bSimST As Boolean = False
            Dim bValBcSt As Boolean = False
            Dim matriz(), matrizComRed(), matrizAliqIcms(), matrizRevenda(), matrizUsoConsu(), matrizAtivo() As String
            Dim j As Integer
            matriz = sCstRegra.Split(",")

            Dim bValComReducao As Boolean = False
            matrizComRed = sCstComReducao.Split(",")

            Dim bValAliqIcms As Boolean = False
            matrizAliqIcms = sCstComAliqIcms.Split(",")

            Dim bValRevenda As Boolean = False
            matrizRevenda = sSitMercRevRegra.Split(",")

            Dim bValUsoConsu As Boolean = False
            matrizUsoConsu = sSitMercUsoRegra.Split(",")

            Dim bValAtivo As Boolean = False
            matrizAtivo = sSitMercAtivoRegra.Split(",")

            Dim bValUnd As Boolean = True

            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vNF", ns)
            dValTNFiscal = node.InnerXml.ToString.Replace(".", ",")

            If bValEnviado Then
                While Not bSai
                    i = i + 1
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:xProd", ns)
                    If node Is Nothing Then
                        bSai = True
                        i = i - 1
                        ReDim Preserve sCodXDesc(1, (i - 1))
                        Exit While
                    Else
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:CFOP", ns)
                        sCompCfop = node.InnerXml.Substring(1, 3)
                        If Not sIssRegra.Contains(sCompCfop) Then
                            If bValServico Then
                                iItemServ = i
                                bValServico = False
                            End If
                        End If

                        bValTemRegra = False
                        bProdCad = True


                        If iSitAtu = 1 Then
                            sIcms = FPegaIcmsSn(xmlDoc, i)
                        ElseIf iSitAtu = 2 Or iSitAtu = 3 Then
                            sIcms = FPegaIcms(xmlDoc, i)
                        End If
                        If iSitAtu = 1 Then
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:CSOSN", ns)
                        Else
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:CST", ns)
                        End If
                        If Not node Is Nothing Then
                            sCst = node.InnerXml.ToString.Replace(".", ",")
                        Else
                            sCst = ""
                        End If

                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:cProd", ns)
                        sCodProd = Strings.Left(node.InnerXml.ToString, 20)
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:xProd", ns)
                        sNomeProd = node.InnerXml.ToString

                        bSimST = False
                        bValMvaSt = False
                        bValBcSt = False
                        For j = 0 To matriz.GetUpperBound(0)
                            If sCst = matriz(j) Then
                                bSimST = True
                                bValMvaSt = True
                                bValBcSt = True
                            End If
                        Next

                        bValComReducao = False
                        For j = 0 To matrizComRed.GetUpperBound(0)
                            If sCst = matrizComRed(j) Then
                                bValComReducao = True
                            End If
                        Next

                        bValAliqIcms = False
                        For j = 0 To matrizAliqIcms.GetUpperBound(0)
                            If sCst = matrizAliqIcms(j) Then
                                bValAliqIcms = True
                            End If
                        Next

                        sCodXDesc(0, (i - 1)) = sCodProd
                        sCodXDesc(1, (i - 1)) = sNomeProd


                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmdSa As New SqlCommand
                            cmdSa.Connection = conError
                            'Pega o código da tabela SB1 utilizado pela empresa
                            cmdSa.CommandText = "SELECT X2_ARQUIVO FROM  SX2" & sCodTabEmitDest & "0 WHERE X2_CHAVE = 'SB1'"
                            Dim drSX2 As SqlDataReader = cmdSa.ExecuteReader()
                            drSX2.Read()
                            If drSX2.HasRows Then


                                TabSb1 = drSX2.Item(0).ToString()
                            End If
                            oReader.Close()
                            conError.Dispose()
                            OnElapsedTime(Me, e)
                        End Using


                        If sTpNf = "ENTRADA" Then
                            Using con As SqlConnection = GetConnectionERP() 'Verifica amarração Prod X Fornecedor
                                Try

                                    con.Open()
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "SELECT B1_COD FROM " & TabSb1 & " WHERE B1_POSIPI = '" & prodNCM & "' AND B1_FILIAL = '" & sFilialEmitDest & "' AND D_E_L_E_T_ <> '*'"

                                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                                    dr.Read()
                                    If dr.HasRows Then
                                        sIdPrd = dr.Item(0).ToString.Replace(" ", "")
                                    Else
                                        bProdCad = False
                                        bProdXFor = False
                                    End If
                                Catch ex As Exception
                                    Using conError As SqlConnection = GetConnectionXML()
                                        conError.Open()
                                        Dim cmd As New SqlCommand
                                        cmd.Connection = conError
                                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                        cmd.ExecuteReader()
                                        oReader.Close()
                                        conError.Dispose()
                                        OnElapsedTime(Me, e)
                                    End Using
                                End Try
                                con.Dispose()
                            End Using
                        ElseIf sTpNf = "TRANSF_ENTSAI" Or sTpNf = "TRANSF_ENT" Or sTpNf = "ENT_IMP" Or sTpNf = "SAIDA" Then
                            Using con As SqlConnection = GetConnectionERP() 'Verifica o cadastro do produto
                                Try
                                    Using conError As SqlConnection = GetConnectionXML()
                                        conError.Open()
                                        Dim cmdSa As New SqlCommand
                                        cmdSa.Connection = conError
                                        'Pega o código da tabela SB1 utilizado pela empresa
                                        cmdSa.CommandText = "SELECT X2_ARQUIVO FROM  " & sCodTabEmitDest & " WHERE X2_CHAVE = 'SB1'"
                                        Dim drSX2 As SqlDataReader = cmdSa.ExecuteReader()

                                        If drSX2.HasRows Then
                                            TabSb1 = drSX2.Item(0).ToString()
                                        End If
                                        oReader.Close()
                                        conError.Dispose()
                                        OnElapsedTime(Me, e)
                                    End Using

                                    con.Open()
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "SELECT B1_COD FROM " & TabSb1 & " WHERE B1_POSIPI = '" & prodNCM & "' AND B1_FILIAL = '" & sFilialEmitDest & "' AND D_E_L_E_T_ <> '*'"
                                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                                    dr.Read()
                                    If dr.HasRows Then
                                        sIdPrd = dr.Item(0)
                                    Else
                                        bProdCad = False
                                    End If
                                Catch ex As Exception
                                    Using conError As SqlConnection = GetConnectionXML()
                                        conError.Open()
                                        Dim cmd As New SqlCommand
                                        cmd.Connection = conError
                                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                        cmd.ExecuteReader()
                                        oReader.Close()
                                        conError.Dispose()
                                        OnElapsedTime(Me, e)
                                    End Using
                                End Try
                                con.Dispose()
                            End Using
                        End If

                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:uCom", ns)
                        sUnd = node.InnerXml.ToString
                        'Dim bValUnd As Boolean = True
                        Dim bValUndSist As Boolean = True
                        Using con As SqlConnection = GetConnectionXML()
                            Try
                                con.Open()
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                cmd.CommandText = "SELECT UNDMED FROM UNDMED WHERE UNDMEDGB = '" & sUnd & "' "
                                Dim dr As SqlDataReader = cmd.ExecuteReader()
                                dr.Read()
                                If dr.HasRows Then
                                    sUnd = dr.Item(0).ToString
                                Else
                                    bValUnd = False
                                End If
                            Catch ex As Exception
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                cmd.ExecuteReader()
                                oReader.Close()
                                con.Dispose()
                                OnElapsedTime(Me, e)
                            End Try
                            con.Dispose()
                        End Using
                        If Not bValUnd Then
                            bVal = False
                            Using con As SqlConnection = GetConnectionXML()
                                Try
                                    con.Open()
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO) VALUES " +
                                        "('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmit & "', '" & sNomeEmit & "', 'CMP', '" & i & "', " +
                                        "'" & sCodProd & "', '" & sNomeProd & "', '" & sUnd & "', 'UNIDADE DE MEDIDA NAO CADASTRADA NA TABELA DE DE->PARA (UNDMED)', 'C')"
                                    cmd.ExecuteReader()
                                Catch ex As Exception
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                    cmd.ExecuteReader()
                                    oReader.Close()
                                    con.Dispose()
                                    OnElapsedTime(Me, e)
                                End Try
                                con.Dispose()
                            End Using
                        Else
                            Using con As SqlConnection = GetConnectionERP() 'Verifica o cadastro do produto
                                Try
                                    con.Open()
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "SELECT AH_UNIMED FROM SAH" & sCodTabEmitDest & "0 WHERE AH_UNIMED = '" & sUnd & "' AND D_E_L_E_T_ <> '*'"
                                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                                    dr.Read()
                                    If dr.HasRows Then
                                        bValUndSist = True
                                    Else
                                        bValUndSist = False
                                    End If
                                Catch ex As Exception
                                    Using conError As SqlConnection = GetConnectionXML()
                                        conError.Open()
                                        Dim cmd As New SqlCommand
                                        cmd.Connection = conError
                                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                        cmd.ExecuteReader()
                                        oReader.Close()
                                        conError.Dispose()
                                        OnElapsedTime(Me, e)
                                    End Using
                                End Try
                                con.Dispose()
                            End Using
                        End If
                        If Not bValUndSist Then
                            bVal = False
                            Using con As SqlConnection = GetConnectionXML()
                                Try
                                    con.Open()
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO) VALUES " +
                                        "('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmit & "', '" & sNomeEmit & "', 'CMP', '" & i & "', '" & sCodProd & "', '" & sNomeProd & "', '" & sUnd & "', 'UNIDADE DE MEDIDA NAO CADASTRADA NO SISTEMA', 'C')"
                                    cmd.ExecuteReader()
                                Catch ex As Exception
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                    cmd.ExecuteReader()
                                    oReader.Close()
                                    con.Dispose()
                                    OnElapsedTime(Me, e)
                                End Try
                                con.Dispose()
                            End Using
                        End If

                        Dim sGrpTrib As String = ""

                        If bProdCad Then
                            Using con As SqlConnection = GetConnectionERP() 'Verifica o cadastro do produto
                                Try
                                    con.Open()
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "SELECT B1_GRTRIB FROM SB1" & sCodTabEmitDest & "0 WHERE B1_COD = '" & sIdPrd & "' AND B1_FILIAL = '" & sFilialEmitDest & "' AND D_E_L_E_T_ <> '*'"
                                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                                    dr.Read()
                                    If dr.HasRows Then
                                        sGrpTrib = dr.Item(0).ToString.Replace(" ", "")
                                    Else
                                        bValGrpTrib = False
                                    End If
                                Catch ex As Exception
                                    Using conError As SqlConnection = GetConnectionXML()
                                        conError.Open()
                                        Dim cmd As New SqlCommand
                                        cmd.Connection = conError
                                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                        cmd.ExecuteReader()
                                        oReader.Close()
                                        conError.Dispose()
                                        OnElapsedTime(Me, e)
                                    End Using
                                End Try
                                con.Dispose()
                            End Using
                            If Not bValGrpTrib Then
                                bVal = False
                                Using con As SqlConnection = GetConnectionXML()
                                    Try
                                        con.Open()
                                        Dim cmd As New SqlCommand
                                        cmd.Connection = con
                                        cmd.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO) VALUES " +
                                            "('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmit & "', '" & sNomeEmit & "', 'CMP', '" & i & "', '" & sCodProd & "', '" & sNomeProd & "', '" & sUnd & "', 'GRUPO DE TRIBUTO NAO CADASTRADO PARA O PRODUTO', 'C')"
                                        cmd.ExecuteReader()
                                    Catch ex As Exception
                                        Dim cmd As New SqlCommand
                                        cmd.Connection = con
                                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                        cmd.ExecuteReader()
                                        oReader.Close()
                                        con.Dispose()
                                        OnElapsedTime(Me, e)
                                    End Try
                                    con.Dispose()
                                End Using
                            End If

                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:CFOP", ns)
                            sCompCfop = node.InnerXml.Substring(1, 3)

                            Dim sTipoEntSai As String = ""
                            If sTpNf = "TRANSF_ENTSAI" Or sTpNf = "SAIDA" Then
                                sTipoEntSai = "B1.B1_TS"
                            Else
                                sTipoEntSai = "B1.B1_TE"
                            End If

                            Using con As SqlConnection = GetConnectionERP()
                                Try
                                    con.Open()
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "SELECT SUBSTRING(F4_CF,2,3) AS COMPCFOP FROM SF4" & sCodTabEmitDest & "0 F4 INNER JOIN SB1" & sCodTabEmitDest & "0 B1 ON F4.F4_CODIGO = " & sTipoEntSai & " WHERE " +
                                        "B1.B1_COD = '" & sIdPrd & "' AND B1.B1_FILIAL = '" & sFilialEmitDest & "' AND F4.F4_FILIAL = '" & sFilialEmitDest & "' AND B1.D_E_L_E_T_ <> '*' AND F4.D_E_L_E_T_ <> '*'"
                                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                                    dr.Read()
                                    If dr.HasRows Then
                                        If Not IsDBNull(dr.Item(0)) And Not dr.Item(0).ToString = "" Then
                                            sCompCfop = dr.Item(0).ToString
                                            bValTemRegra = True
                                        Else
                                            bValTemRegra = False
                                        End If
                                    Else
                                        bValTemRegra = False
                                    End If
                                Catch ex As Exception
                                    Using conError As SqlConnection = GetConnectionXML()
                                        conError.Open()
                                        Dim cmd As New SqlCommand
                                        cmd.Connection = conError
                                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                        cmd.ExecuteReader()
                                        oReader.Close()
                                        conError.Dispose()
                                        OnElapsedTime(Me, e)
                                    End Using
                                End Try
                                con.Dispose()
                            End Using

                            bValRevenda = False
                            For j = 0 To matrizRevenda.GetUpperBound(0)
                                If sCompCfop = matrizRevenda(j) Then
                                    bValRevenda = True
                                End If
                            Next

                            bValUsoConsu = False
                            For j = 0 To matrizUsoConsu.GetUpperBound(0)
                                If sCompCfop = matrizUsoConsu(j) Then
                                    bValUsoConsu = True
                                End If
                            Next

                            bValAtivo = False
                            For j = 0 To matrizAtivo.GetUpperBound(0)
                                If sCompCfop = matrizAtivo(j) Then
                                    bValAtivo = True
                                End If
                            Next


                            bValCadProd = True
                            Dim sCfop As String = sDFEst & "." & sCompCfop
                            Dim iIdRegraIcms As Integer
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vProd", ns)
                            If Not node Is Nothing Then
                                dValProd = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dValProd = 0
                            End If
                            dValTPrd += Round(dValProd, 2)
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vFrete", ns)
                            If Not node Is Nothing Then
                                dValFrete = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dValFrete = 0
                            End If
                            dValTFrete += Round(dValFrete, 2)
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vDesc", ns)
                            If Not node Is Nothing Then
                                dValDesc = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dValDesc = 0
                            End If
                            dValTDesc += Round(dValDesc, 2)
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vOutro", ns)
                            If Not node Is Nothing Then
                                dValOutro = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dValOutro = 0
                            End If
                            dValTOutraDesp += Round(dValOutro, 2)
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vSeg", ns)
                            If Not node Is Nothing Then
                                dValSeg = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dValSeg = 0
                            End If
                            dValTSeguro += Round(dValSeg, 2)
                            If iSitAtu = 1 Then
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pCredSN", ns)
                            Else
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pICMS", ns)
                            End If
                            If Not node Is Nothing Then
                                dAliqIcms = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dAliqIcms = 0
                            End If

                            If iSitAtu = 1 Then
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:vCredICMSSN", ns)
                            Else
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:vICMS", ns)
                            End If
                            If Not node Is Nothing Then
                                dVIcms = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dVIcms = 0
                            End If
                            dValTIcms += Round(dVIcms, 2)
                            If iSitAtu = 1 Then
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:vBC", ns)
                            Else
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:vBC", ns)
                            End If
                            If Not node Is Nothing Then
                                dBc = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dBc = 0
                            End If
                            If iSitAtu = 1 Then
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pRedBC", ns)
                            Else
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pRedBC", ns)
                            End If
                            If Not node Is Nothing Then
                                bRedBC = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                bRedBC = 0
                            End If
                            If iSitAtu = 1 Then
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pRedBCST", ns)
                            Else
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pRedBCST", ns)
                            End If
                            If Not node Is Nothing Then
                                bRedBCST = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                bRedBCST = 0
                            End If
                            If iSitAtu = 1 Then
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pMVAST", ns)
                            Else
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pMVAST", ns)
                            End If
                            If Not node Is Nothing Then
                                dMvaST = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dMvaST = 0
                            End If
                            If iSitAtu = 1 Then
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:vBCST", ns)
                            Else
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:vBCST", ns)
                            End If
                            If Not node Is Nothing Then
                                dBcST = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dBcST = 0
                            End If
                            If iSitAtu = 1 Then
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:vCredICMSSN", ns)
                            Else
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:vICMSST", ns)
                            End If
                            If Not node Is Nothing Then
                                dVIcmsSt = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dVIcmsSt = 0
                            End If
                            dValTICMSST += Round(dVIcmsSt, 2)

                            If iSitAtu = 1 Then
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pICMSST", ns)
                            Else
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pICMSST", ns)
                            End If
                            If Not node Is Nothing Then
                                dAliqIcmsSt = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dAliqIcmsSt = 0
                            End If

                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:IPI/nfe:IPITrib/nfe:pIPI", ns)
                            If Not node Is Nothing Then
                                dIpi = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dIpi = 0
                            End If
                            If dIpi > 0 Then
                                dValIpi = 1
                            Else
                                dValIpi = 0
                            End If
                            dValTIPI += Round(dValProd * (dIpi / 100), 2)
                            If dIpi > 0 Then
                                dValIpi = 1
                            Else
                                dValIpi = 0
                            End If
                            Dim sRegraIcms As String = ""
                            Dim dVPrecoPauta, dVAliqPauta, dBcSTPauta, dQuant As Double
                            Dim dBcCalc, dValIcmsCalc, dBcSTCalc, dValIcmsSTCalc, dFatorMva As Double
                            Using con4 As SqlConnection = GetConnectionERP()
                                Try
                                    con4.Open()
                                    Dim cmd4 As New SqlCommand
                                    cmd4.Connection = con4
                                    cmd4.CommandText = "SELECT F7_MARGEM, F7_VLR_ICM, F7_VLRICMP FROM SF7" & sCodTabEmitDest & "0 WHERE F7_GRTRIB = '" & sGrpTrib & "' AND F7_FILIAL = '" & sFilialEmitDest & "' AND F7_EST = '" & sUFEmitDest & "' AND D_E_L_E_T_ <> '*'"
                                    Dim dr4 As SqlDataReader = cmd4.ExecuteReader()
                                    dr4.Read()
                                    If dr4.HasRows Then
                                        If IsDBNull(dr4.Item(0)) Then
                                            dFatorMva = 0
                                            dVPrecoPauta = 0
                                            dVAliqPauta = 0
                                        Else
                                            dFatorMva = dr4.Item(0)
                                            dVAliqPauta = dr4.Item(1)
                                            dVPrecoPauta = dr4.Item(2)
                                        End If
                                    Else
                                        dFatorMva = 0
                                        dVPrecoPauta = 0
                                        dVAliqPauta = 0
                                    End If
                                Catch ex As Exception
                                    Using conError As SqlConnection = GetConnectionXML()
                                        conError.Open()
                                        Dim cmd As New SqlCommand
                                        cmd.Connection = conError
                                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                        cmd.ExecuteReader()
                                        oReader.Close()
                                        conError.Dispose()
                                        OnElapsedTime(Me, e)
                                    End Using
                                End Try
                                con4.Dispose()
                            End Using
                            If iSitAtu = 1 Then
                                Select Case sCst
                                    Case "101"
                                        sIcmsSnxLr = "00"
                                    Case "102"
                                        sIcmsSnxLr = "41"
                                    Case "103"
                                        sIcmsSnxLr = "40"
                                    Case "201"
                                        sIcmsSnxLr = "10"
                                    Case "202"
                                        sIcmsSnxLr = "30"
                                    Case "203"
                                        sIcmsSnxLr = "30"
                                    Case "300"
                                        sIcmsSnxLr = "40"
                                    Case "400"
                                        sIcmsSnxLr = "41"
                                    Case "500"
                                        sIcmsSnxLr = "60"
                                    Case "900"
                                        sIcmsSnxLr = "90"
                                End Select
                                If Not sIcmsSnxLr = "" Then
                                    sIcmsTemp = sCst
                                    sCst = sIcmsSnxLr
                                End If
                            End If

                            If Not sTpNf = "ENT_IMP" Then
                                If Not dVPrecoPauta > 0 Then
                                    If dMvaST = 0 And bValMvaSt Then
                                        bVal = False
                                        Using con4 As SqlConnection = GetConnectionXML()
                                            Try
                                                con4.Open()
                                                Dim cmd4 As New SqlCommand '             
                                                cmd4.Connection = con4
                                                cmd4.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, VALOR_PRD, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, CRITICA, TIPO) " +
                                                    "VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', '" & i & "', '" & sIdPrd & "', '" & sCodProd & "', " +
                                                    "'" & sNomeProd & "', '" & Str(dValProd) & "', '" & sCst & "', '" & Str(dAliqIcms) & "', '" & Str(dAliqIcmsSt) & "', '" & Str(bRedBC) & "', 'MVA NAO DESTACADO NO XML, CST UTILIZADO OBRIGA PREENCHIMENTO DE INFORMACOES ST', 'A')"
                                                cmd4.ExecuteReader()
                                            Catch ex As Exception
                                                Dim cmd4 As New SqlCommand
                                                cmd4.Connection = con4
                                                cmd4.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                cmd4.ExecuteReader()
                                                oReader.Close()
                                                con4.Dispose()
                                                OnElapsedTime(Me, e)
                                            End Try
                                            con4.Dispose()
                                        End Using
                                    ElseIf dMvaST <> dFatorMva And (dMvaST > 0) Then
                                        bVal = False
                                        Using con4 As SqlConnection = GetConnectionXML()
                                            Try
                                                con4.Open()
                                                Dim cmd4 As New SqlCommand
                                                cmd4.Connection = con4
                                                cmd4.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, VALOR_PRD, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, VALOR_MVA_XML, " +
                                                    "VALOR_MVA_PRD, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', '" & i & "', '" & sIdPrd & "', '" & sCodProd & "', " +
                                                    "'" & sNomeProd & "', '" & Str(dValProd) & "', '" & sCst & "', '" & Str(dAliqIcms) & "', '" & Str(dAliqIcmsSt) & "', '" & Str(bRedBC) & "', '" & Str(dMvaST) & "', '" & Str(dFatorMva) & "', " +
                                                    "'DIVERGENCIA NO VALOR MVA XML X MVA CADASTRADO NAS EXCECOES FISCAIS', 'A')"
                                                cmd4.ExecuteReader()
                                            Catch ex As Exception
                                                Dim cmd4 As New SqlCommand
                                                cmd4.Connection = con4
                                                cmd4.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                cmd4.ExecuteReader()
                                                oReader.Close()
                                                con4.Dispose()
                                                OnElapsedTime(Me, e)
                                            End Try
                                            con4.Dispose()
                                        End Using
                                    End If
                                End If

                                If bRedBC > 0 And bValComReducao Then 'Calcula BC com reducao
                                    dBcCalc = dValProd - (dValProd * (bRedBC / 100))
                                    If Not dVPrecoPauta > 0 Then
                                        If Not dBc = Round(dBcCalc, 2) Then 'Valida BC Reduzida
                                            bVal = False
                                            Using con4 As SqlConnection = GetConnectionXML()
                                                Try
                                                    con4.Open()
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, VALOR_PRD, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, " +
                                                        "VALOR_BCRED_XML, VALOR_BCRED_CALC, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', '" & i & "', " +
                                                        "'" & sIdPrd & "', '" & sCodProd & "', '" & sNomeProd & "', '" & Str(dValProd) & "', '" & sCst & "', '" & Str(dAliqIcms) & "', '" & Str(dAliqIcmsSt) & "', '" & Str(bRedBC) & "', '" & Str(dBc) & "', " +
                                                        "'" & Str(Round(dBcCalc, 2)) & "', 'DIVERGENCIA NA BC COM REDUCAO XML X BC COM REDUCAO CALCULADA', 'A')"
                                                    cmd4.ExecuteReader()
                                                Catch ex As Exception
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                    cmd4.ExecuteReader()
                                                    oReader.Close()
                                                    con4.Dispose()
                                                    OnElapsedTime(Me, e)
                                                End Try
                                                con4.Dispose()
                                            End Using
                                        End If
                                    End If
                                    dValIcmsCalc = dBcCalc * (dAliqIcms / 100)
                                    If Not dVPrecoPauta > 0 Then
                                        If Not dVIcms = Round(dValIcmsCalc, 2) Then 'Valida valor do ICMS
                                            bVal = False
                                            Using con4 As SqlConnection = GetConnectionXML()
                                                Try
                                                    con4.Open()
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, VALOR_PRD, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, " +
                                                        "VALOR_ICMS_XML, VALOR_ICMS_CALC, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', '" & i & "', " +
                                                        "'" & sIdPrd & "', '" & sCodProd & "', '" & sNomeProd & "', '" & Str(dValProd) & "', '" & sCst & "', '" & Str(dAliqIcms) & "', '" & Str(dAliqIcmsSt) & "', '" & Str(bRedBC) & "', '" & Str(dVIcms) & "', " +
                                                        "'" & Str(Round(dValIcmsCalc, 2)) & "', 'DIVERGENCIA NO ICMS COM BASE REDUZIDA XML X ICMS COM BASE REDUZIDA CALCULADO', 'A')"
                                                    cmd4.ExecuteReader()
                                                Catch ex As Exception
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                    cmd4.ExecuteReader()
                                                    oReader.Close()
                                                    con4.Dispose()
                                                    OnElapsedTime(Me, e)
                                                End Try
                                                con4.Dispose()
                                            End Using
                                        End If
                                    End If
                                Else
                                    Dim dValIpiCalc, dValorFrete, dValorDesc, dValorOutro, dValorSeg, dValorIpi As Double
                                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vFrete", ns)
                                    If Not node Is Nothing Then
                                        dValorFrete = node.InnerXml.ToString.Replace(".", ",")
                                    Else
                                        dValorFrete = 0
                                    End If
                                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vDesc", ns)
                                    If Not node Is Nothing Then
                                        dValorDesc = node.InnerXml.ToString.Replace(".", ",")
                                    Else
                                        dValorDesc = 0
                                    End If
                                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vOutro", ns)
                                    If Not node Is Nothing Then
                                        dValorOutro = node.InnerXml.ToString.Replace(".", ",")
                                    Else
                                        dValorOutro = 0
                                    End If
                                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vSeg", ns)
                                    If Not node Is Nothing Then
                                        dValorSeg = node.InnerXml.ToString.Replace(".", ",")
                                    Else
                                        dValorSeg = 0
                                    End If
                                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:IPI/nfe:IPITrib/nfe:vIPI", ns)
                                    If Not node Is Nothing Then
                                        dValorIpi = node.InnerXml.ToString.Replace(".", ",")
                                    Else
                                        dValorIpi = 0
                                    End If
                                    If bValAtivo Or bValUsoConsu Then
                                        dValIpiCalc = dValProd * (dIpi / 100)
                                    Else
                                        dValIpiCalc = 0
                                    End If
                                    If Not dVPrecoPauta > 0 Then
                                        If Not dValorIpi = Round(dValIpiCalc, 2) Then 'Valida valor do IPI
                                            Dim dIpiCalc As Double = dValorIpi - Round(dValIpiCalc, 2)
                                            If Not Round(dIpiCalc) = 0.0 Then
                                                bVal = False
                                                Using con4 As SqlConnection = GetConnectionXML()
                                                    Try
                                                        con4.Open()
                                                        Dim cmd4 As New SqlCommand
                                                        cmd4.Connection = con4
                                                        cmd4.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, VALOR_PRD, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, " +
                                                            "VALOR_ICMS_XML, VALOR_ICMS_CALC, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', '" & i & "', " +
                                                            "'" & sIdPrd & "', '" & sCodProd & "', '" & sNomeProd & "', '" & Str(dValProd) & "', '" & sCst & "', '" & Str(dAliqIcms) & "', '" & Str(dAliqIcmsSt) & "', '" & Str(bRedBC) & "', '" & Str(dVIcms) & "', " +
                                                            "'" & Str(Round(dValIcmsCalc, 2)) & "', 'DIVERGENCIA NO IPI (" & dValorIpi & ") INFORMADO DO XML X COM O VALOR DO IPI (" & Round(dValIpiCalc, 2) & ") CALCULADO', 'A')"
                                                        cmd4.ExecuteReader()
                                                    Catch ex As Exception
                                                        Dim cmd4 As New SqlCommand
                                                        cmd4.Connection = con4
                                                        cmd4.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                        cmd4.ExecuteReader()
                                                        oReader.Close()
                                                        con4.Dispose()
                                                        OnElapsedTime(Me, e)
                                                    End Try
                                                    con4.Dispose()
                                                End Using
                                            End If
                                        End If
                                    End If
                                    If sTpNf = "ENT_IMP" Then
                                        Dim dValBcImp As Double
                                        dValBcImp = dValProd / ((100 - dAliqIcms) / 100)
                                        dValIcmsCalc = Round(dValBcImp, 2) * (dAliqIcms / 100)
                                        If Not dVIcms = Round(dValIcmsCalc, 2) Then 'Valida valor do ICMS
                                            bVal = False
                                            Using con4 As SqlConnection = GetConnectionXML()
                                                Try
                                                    con4.Open()
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, VALOR_PRD, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, " +
                                                        "VALOR_ICMS_XML, VALOR_ICMS_CALC, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', '" & i & "', " +
                                                        "'" & sIdPrd & "', '" & sCodProd & "', '" & sNomeProd & "', '" & Str(dValProd) & "', '" & sCst & "', '" & Str(dAliqIcms) & "', '" & Str(dAliqIcmsSt) & "', '" & Str(bRedBC) & "', '" & Str(dVIcms) & "', " +
                                                        "'" & Str(Round(dValIcmsCalc, 2)) & "', 'DIVERGENCIA NO ICMS SEM BASE REDUZIDA XML X ICMS SEM BASE REDUZIDA CALCULADO', 'A')"
                                                    cmd4.ExecuteReader()
                                                Catch ex As Exception
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                    cmd4.ExecuteReader()
                                                    oReader.Close()
                                                    con4.Dispose()
                                                    OnElapsedTime(Me, e)
                                                End Try
                                                con4.Dispose()
                                            End Using
                                        End If
                                    Else
                                        If sCompCfop = sUsoComStRegra Or sCompCfop = sUsoSemStRegra Or sCompCfop = sMercRevComStRegra Or sCompCfop = sMercRevSemStRegra Then
                                            dValIcmsCalc = (dValProd + dValIpiCalc + dValorFrete + dValorOutro + dValorSeg - dValorDesc) * (dAliqIcms / 100)
                                        Else
                                            dValIcmsCalc = (dValProd + dValorFrete + dValorOutro + dValorSeg - dValorDesc) * (dAliqIcms / 100)
                                        End If
                                        If Not dVIcms = Round(dValIcmsCalc, 2) Then 'Valida valor do ICMS
                                            bVal = False
                                            Using con4 As SqlConnection = GetConnectionXML()
                                                Try
                                                    con4.Open()
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, VALOR_PRD, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, " +
                                                        "VALOR_ICMS_XML, VALOR_ICMS_CALC, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', '" & i & "', " +
                                                        "'" & sIdPrd & "', '" & sCodProd & "', '" & sNomeProd & "', '" & Str(dValProd) & "', '" & sCst & "', '" & Str(dAliqIcms) & "', '" & Str(dAliqIcmsSt) & "', '" & Str(bRedBC) & "', '" & Str(dVIcms) & "', " +
                                                        "'" & Str(Round(dValIcmsCalc, 2)) & "', 'DIVERGENCIA NO ICMS SEM BASE REDUZIDA XML X ICMS SEM BASE REDUZIDA CALCULADO', 'A')"
                                                    cmd4.ExecuteReader()
                                                Catch ex As Exception
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                    cmd4.ExecuteReader()
                                                    oReader.Close()
                                                    con4.Dispose()
                                                    OnElapsedTime(Me, e)
                                                End Try
                                                con4.Dispose()
                                            End Using
                                        End If
                                    End If
                                End If

                                If bValBcSt Then 'BC ST 
                                    If bRedBCST > 0 Then
                                        dBcSTCalc = (dValProd + (dValProd * (dMvaST / 100))) - ((dValProd + (dValProd * (dMvaST / 100))) * (bRedBCST / 100))
                                    Else
                                        dBcSTCalc = dValProd + (dValProd * (dMvaST / 100))
                                    End If
                                    If Not dVPrecoPauta > 0 Then
                                        If Not dBcST = Round(dBcSTCalc, 2) Then 'Valida BC ST
                                            bVal = False
                                            Using con4 As SqlConnection = GetConnectionXML()
                                                Try
                                                    con4.Open()
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, VALOR_PRD, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, " +
                                                        "VALOR_BCST_XML, VALOR_BCST_CALC, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', '" & i & "', " +
                                                        "'" & sIdPrd & "', '" & sCodProd & "', '" & sNomeProd & "', '" & Str(dValProd) & "', '" & sCst & "', '" & Str(dAliqIcms) & "', '" & Str(dAliqIcmsSt) & "', '" & Str(bRedBC) & "', '" & Str(dBcST) & "', " +
                                                        "'" & Str(Round(dBcSTCalc, 2)) & "', 'DIVERGENCIA NA BC ST COM MVA XML X BC ST COM MVA CALCULADA', 'A')"
                                                    cmd4.ExecuteReader()
                                                Catch ex As Exception
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                    cmd4.ExecuteReader()
                                                    oReader.Close()
                                                    con4.Dispose()
                                                    OnElapsedTime(Me, e)
                                                End Try
                                                con4.Dispose()
                                            End Using
                                        End If
                                    Else
                                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:qCom", ns)
                                        dQuant = node.InnerXml.ToString.Replace(".", ",")     'Quantidade
                                        dBcSTPauta = dVPrecoPauta * dQuant
                                        If Not dBcST = Round(dBcSTPauta, 2) Then  'Valida BC ST com Pauta
                                            bVal = False
                                            Using con4 As SqlConnection = GetConnectionXML()
                                                Try
                                                    con4.Open()
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, VALOR_PRD, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, " +
                                                        "VALOR_BCST_XML, VALOR_BCST_CALC, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', '" & i & "', " +
                                                        "'" & sIdPrd & "', '" & sCodProd & "', '" & sNomeProd & "', '" & Str(dValProd) & "', '" & sCst & "', '" & Str(dAliqIcms) & "', '" & Str(dAliqIcmsSt) & "', '" & Str(bRedBC) & "', '" & Str(dBcST) & "', " +
                                                        "'" & Str(Round(dBcSTCalc, 2)) & "', 'DIVERGENCIA NA BC ST COM PAUTA XML X BC ST COM PAUTA CALCULADA', 'A')"
                                                    cmd4.ExecuteReader()
                                                Catch ex As Exception
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                    cmd4.ExecuteReader()
                                                    oReader.Close()
                                                    con4.Dispose()
                                                    OnElapsedTime(Me, e)
                                                End Try
                                                con4.Dispose()
                                            End Using
                                        End If
                                    End If
                                    If iSitAtu = 1 Then
                                        dValIcmsSTCalc = (dBcSTCalc * (dAliqIcmsSt / 100)) - (dValProd * (dAliqIcmsSt / 100))
                                    Else
                                        dValIcmsSTCalc = (dBcSTCalc * (dAliqIcmsSt / 100)) - dValIcmsCalc
                                    End If
                                    If Not dVPrecoPauta > 0 Then
                                        If Not dVIcmsSt = Round(dValIcmsSTCalc, 2) Then 'Valida valor do ICMSST
                                            bVal = False
                                            Using con4 As SqlConnection = GetConnectionXML()
                                                Try
                                                    con4.Open()
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, VALOR_PRD, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, " +
                                                        "VALOR_ICMSST_XML, VALOR_ICMSST_CALC, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', '" & i & "', " +
                                                        "'" & sIdPrd & "', '" & sCodProd & "', '" & sNomeProd & "', '" & Str(dValProd) & "', '" & sCst & "', '" & Str(dAliqIcms) & "', '" & Str(dAliqIcmsSt) & "', '" & Str(bRedBC) & "', " +
                                                        "'" & Str(dVIcmsSt) & "', '" & Str(Round(dValIcmsSTCalc, 2)) & "', 'DIVERGENCIA NO ICMS ST COM MVA XML X ICMS ST COM MVA CALCULADO', 'A')"
                                                    cmd4.ExecuteReader()
                                                Catch ex As Exception
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                    cmd4.ExecuteReader()
                                                    oReader.Close()
                                                    con4.Dispose()
                                                    OnElapsedTime(Me, e)
                                                End Try
                                                con4.Dispose()
                                            End Using
                                        End If
                                        dValTICMSSTCalc += Round(dValIcmsSTCalc, 2)
                                    Else
                                        Dim dVIcmsStPauta As Double
                                        dVIcmsStPauta = (dBcSTPauta * (dVAliqPauta / 100)) - Round(dValIcmsCalc, 2)
                                        If Not dVIcmsSt = Round(dVIcmsStPauta, 2) Then 'Valida valor do ICMSST
                                            bVal = False
                                            Using con4 As SqlConnection = GetConnectionXML()
                                                Try
                                                    con4.Open()
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, VALOR_PRD, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, " +
                                                        "VALOR_ICMSST_XML, VALOR_ICMSST_CALC, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', '" & i & "', " +
                                                        "'" & sIdPrd & "', '" & sCodProd & "', '" & sNomeProd & "', '" & Str(dValProd) & "', '" & sCst & "', '" & Str(dAliqIcms) & "', '" & Str(dAliqIcmsSt) & "', '" & Str(bRedBC) & "', " +
                                                        "'" & Str(dVIcmsSt) & "', '" & Str(Round(dVIcmsStPauta, 2)) & "', 'DIVERGENCIA NO ICMS ST COM PAUTA XML X ICMS ST COM PAUTA CALCULADO', 'A')"
                                                    cmd4.ExecuteReader()
                                                Catch ex As Exception
                                                    Dim cmd4 As New SqlCommand
                                                    cmd4.Connection = con4
                                                    cmd4.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                    cmd4.ExecuteReader()
                                                    oReader.Close()
                                                    con4.Dispose()
                                                    OnElapsedTime(Me, e)
                                                End Try
                                                con4.Dispose()
                                            End Using
                                        End If
                                        dValTICMSSTCalc += Round(dVIcmsStPauta, 2)
                                    End If
                                End If
                            End If
                        Else
                            bVal = False
                            Using con As SqlConnection = GetConnectionXML()
                                Try
                                    con.Open()
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO) VALUES " +
                                        "('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmit & "', '" & sNomeEmit & "', 'CMP', '" & i & "', '" & sCodProd & "', '" & sNomeProd & "', '" & sUnd & "', 'PRODUTO NAO CADASTRADO', 'C')"
                                    cmd.ExecuteReader()
                                Catch ex As Exception
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                    cmd.ExecuteReader()
                                    oReader.Close()
                                    con.Dispose()
                                    OnElapsedTime(Me, e)
                                End Try
                                con.Dispose()
                            End Using
                        End If
                        If Not bProdXFor Then
                            bVal = False
                            Using con As SqlConnection = GetConnectionXML()
                                Try
                                    con.Open()
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO) VALUES " +
                                        "('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmit & "', '" & sNomeEmit & "', 'CMP', '" & i & "', '" & sCodProd & "', '" & sNomeProd & "', '" & sUnd & "', 'AMARRACAO PRODUTO X FORNECEDOR INEXISTENTE', 'C')"
                                    cmd.ExecuteReader()
                                Catch ex As Exception
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                    cmd.ExecuteReader()
                                    oReader.Close()
                                    con.Dispose()
                                    OnElapsedTime(Me, e)
                                End Try
                                con.Dispose()
                            End Using
                        End If
                    End If

                    If Not bValTemRegra Then
                        bVal = False
                        Using con3 As SqlConnection = GetConnectionXML()
                            Try
                                con3.Open()
                                Dim cmd3 As New SqlCommand
                                cmd3.Connection = con3
                                cmd3.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, VALOR_PRD, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, " +
                                 "CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', '" & i & "', '" & sIdPrd & "', '" & sCodProd & "', " +
                                 "'" & sNomeProd & "', '" & Str(dValProd) & "', '" & sCst & "', '" & Str(dAliqIcms) & "', '" & Str(dAliqIcmsSt) & "', '" & Str(bRedBC) & "', 'TES COM CFOP " & sDFEst & sCompCfop & " E AS CONDICOES INFORMADA NAO CADASTRADA NO SISTEMA', 'C')"
                                cmd3.ExecuteReader()
                            Catch ex As Exception
                                Dim cmd3 As New SqlCommand
                                cmd3.Connection = con3
                                cmd3.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                cmd3.ExecuteReader()
                                oReader.Close()
                                con3.Dispose()
                                OnElapsedTime(Me, e)
                            End Try
                            con3.Dispose()
                        End Using
                    End If

                    If bValAliqIcms Then 'Verifica se a Aliquota do CST está correta
                        If Not sTpNf = "ENT_IMP" Then
                            If iSitAtu <> 1 Then
                                If Not dAliqIcms > 0 Then
                                    bVal = False
                                    Using con As SqlConnection = GetConnectionXML()
                                        Try
                                            con.Open()
                                            Dim cmd As New SqlCommand
                                            cmd.Connection = con
                                            cmd.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, VALOR_PRD, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, " +
                                                "CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', '" & i & "', '" & sIdPrd & "', '" & sCodProd & "', " +
                                                "'" & sNomeProd & "', '" & Str(dValProd) & "', '" & sCst & "', '" & Str(dAliqIcms) & "', '" & Str(dAliqIcmsSt) & "', '" & Str(bRedBC) & "', 'DIVERGENCIA ENTRE CST E ALIQUOTA', 'A')"
                                            cmd.ExecuteReader()
                                        Catch ex As Exception
                                            Dim cmd As New SqlCommand
                                            cmd.Connection = con
                                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                            cmd.ExecuteReader()
                                            oReader.Close()
                                            con.Dispose()
                                            OnElapsedTime(Me, e)
                                        End Try
                                        con.Dispose()
                                    End Using
                                End If
                            End If
                        End If
                    End If
                    ReDim Preserve sCodXDesc(1, i)
                End While
            End If
            For i = 0 To iItens
                For j = 0 To iItens
                    If sCodXDesc(0, i) = sCodXDesc(0, j) And sCodXDesc(1, i) <> sCodXDesc(1, j) Then 'Verifica se a Descricao dos itens repete em cod. de itens diferentes 
                        bVal = False
                        Using con As SqlConnection = GetConnectionXML()
                            Try
                                con.Open()
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                cmd.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CNPJ, RAZAO, COD_PRD_AUX, DESC_PRD, CRITICA, TIPO) VALUES " +
                                    "('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', 'CMP', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', '" & sCodXDesc(0, i) & "', " +
                                    "'" & sCodXDesc(1, i) & "', 'CODIGOS IGUAIS COM DESCRICAO DIFERENTE NO XML', 'C')"
                                cmd.ExecuteReader()
                            Catch ex As Exception
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                cmd.ExecuteReader()
                                oReader.Close()
                                con.Dispose()
                                OnElapsedTime(Me, e)
                            End Try
                            con.Dispose()
                        End Using
                        bValArq = True
                        oReader.Close()
                        Continue For
                    End If
                Next
            Next
            ReDim sCodXDesc(0, 0)
        End If
        If bValCadProd Then
            If Not sTpNf = "ENT_IMP" Then
                dValTNfCalc = (dValTPrd + dValTICMSSTCalc + dValTFrete + dValTSeguro + dValTOutraDesp + dValTIPI) - dValTDesc
                If dValTNFiscal <> Round(dValTNfCalc, 2) Then
                    Dim dValorTotalNF As Double = dValTNFiscal - dValTNfCalc
                    If (Round(dValorTotalNF, 2) = 0.01) Or (Round(dValorTotalNF, 2) = (0.01 * -1)) Then
                        Using con4 As SqlConnection = GetConnectionXML()  'Grava log
                            Try
                                con4.Open()
                                Dim cmd4 As New SqlCommand
                                cmd4.Connection = con4
                                cmd4.CommandText = "INSERT INTO LOGEVENTOSXML (NOME_XML, DATAEMISSAO, SETOR, USUARIO, EVENTO, CRITICA) " +
                                    "VALUES ('" & arq.Name & "',convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), 'FIS', '" & sCodUsuario & "', 'I', 'IMPORTADO COM DIFERENÇA DE 0,01 NO TOTAL DA NF')"
                                cmd4.ExecuteReader()
                            Catch ex As Exception
                                Dim cmd4 As New SqlCommand
                                cmd4.Connection = con4
                                cmd4.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                cmd4.ExecuteReader()
                                oReader.Close()
                                con4.Dispose()
                                OnElapsedTime(Me, e)
                            End Try
                            con4.Dispose()
                        End Using
                    Else
                        bVal = False
                        Using con As SqlConnection = GetConnectionXML()
                            Try
                                con.Open()
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                cmd.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, CRITICA, TIPO) VALUES " +
                                    "('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmitDest & "', '" & sNomeEmitDest & "', 'FIS', 'DIVERGENCIA ENTRE O VALOR DA NF (" & dValTNFiscal & ") COM O VALOR DA NF CALCULADA (" & dValTNfCalc & ") VALIDAR CADASTRO FISCAL DO RM X IMPOSTOS DESTACADOS NA NF RECEBIDA.', 'A')"
                                cmd.ExecuteReader()
                            Catch ex As Exception
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                cmd.ExecuteReader()
                                oReader.Close()
                                con.Dispose()
                                OnElapsedTime(Me, e)
                            End Try
                            con.Dispose()
                        End Using
                    End If
                End If
            End If
        End If
        If bValArq Then
            Using con As SqlConnection = GetConnectionXML()  'Grava log
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO LOGEVENTOSXML (NOME_XML, DATAEMISSAO, SETOR, USUARIO, EVENTO, CRITICA) " +
                        "VALUES ('" & arq.Name & "',convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), 'FIS', '" & sCodUsuario & "', 'I', 'NF DEVERA SER LANCADA MANUALMENTE POIS EXISTEM ITENS COM CODIGOS REPETIDOS COM DESCRICAO DIFERENTE NO ARQUIVO XML. XML TRANSFERIDO PARA A PASTA MANUAL.')"
                    cmd.ExecuteReader()
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con.Dispose()
            End Using
            oReader.Close()
            File.Delete(sManual & "\" & arq.Name)
            File.Copy(caminho & "\" & arq.Name, sManual & "\" & arq.Name)
            File.Delete(caminho & "\" & arq.Name)
        End If
        Return bVal
    End Function

    Function fVerCadCliFor(ByVal e As System.EventArgs)
        fLog("Função", "fVerCadCliFor")
        If bAtivaLog Then
            fLog(arq.Name, "Função fVerCadCliFor")
        End If

        Dim bVal As Boolean = True
        Dim iVal As Integer = 0

        If sTpNf <> "ENT_IMP" And sTpNf <> "ENTRADA" Then
            Using con As SqlConnection = GetConnectionERP()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT A1_MSBLQL FROM SA1" & sCodTabEmitDest & "0 WHERE A1_COD = '" & sCodEmpDest & "' AND D_E_L_E_T_ <> '*'"
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        If Not IsDBNull(dr.Item(0)) And Not dr.Item(0).ToString.Replace(" ", "") = "" Then
                            If dr.Item(0) = 1 Then '2 = DESBLOQUEADO / 1 = BLOQUEADO
                                Using con2 As SqlConnection = GetConnectionXML()
                                    Try
                                        con2.Open()
                                        Dim cmd2 As New SqlCommand
                                        cmd2.Connection = con2
                                        cmd2.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CNPJ, RAZAO, CRITICA, TIPO) VALUES " +
                                            "('" & sFilialDest & "', '" & arq.Name & "', '" & sDataEmissao & "', 'CMP', '" & sCnpjDest & "', " +
                                            "'" & sNomeDest & "', 'CLIENTE BLOQUEADO NO SISTEMA', 'C')"
                                        cmd2.ExecuteReader()
                                    Catch ex2 As Exception
                                        Dim cmd2 As New SqlCommand
                                        cmd2.Connection = con2
                                        cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex2.Message & "')"
                                        cmd2.ExecuteReader()
                                        oReader.Close()
                                        con2.Dispose()
                                        OnElapsedTime(Me, e)
                                    End Try
                                    con2.Dispose()
                                End Using
                                bVal = False
                            End If
                        End If
                    Else
                        Using con2 As SqlConnection = GetConnectionXML()
                            Try
                                con2.Open()
                                Dim cmd2 As New SqlCommand
                                cmd2.Connection = con2
                                cmd2.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CNPJ, RAZAO, CRITICA, TIPO) VALUES " +
                                    "('" & sFilialDest & "', '" & arq.Name & "', '" & sDataEmissao & "', 'CMP', '" & sCnpjDest & "', " +
                                    "'" & sNomeDest & "', 'CLIENTE NAO CADASTRADO NO SISTEMA', 'C')"
                                cmd2.ExecuteReader()
                            Catch ex2 As Exception
                                Dim cmd2 As New SqlCommand
                                cmd2.Connection = con2
                                cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex2.Message & "')"
                                cmd2.ExecuteReader()
                                oReader.Close()
                                con2.Dispose()
                                OnElapsedTime(Me, e)
                            End Try
                            con2.Dispose()
                        End Using
                        bVal = False
                    End If
                    con.Dispose()
                Catch ex As Exception
                    Using conex As SqlConnection = GetConnectionXML()
                        Dim cmd As New SqlCommand
                        cmd.Connection = conex
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        conex.Dispose()
                        OnElapsedTime(Me, e)
                    End Using
                End Try
            End Using
        End If
        Using con As SqlConnection = GetConnectionERP()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT A2_MSBLQL FROM SA2" & sCodTabEmitDest & "0 WHERE A2_COD = '" & sCodEmpEmit & "' AND D_E_L_E_T_ <> '*'"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                dr.Read()
                If dr.HasRows Then
                    If Not IsDBNull(dr.Item(0)) And Not dr.Item(0).ToString.Replace(" ", "") = "" Then
                        If dr.Item(0) = 1 Then '2 = DESBLOQUEADO / 1 = BLOQUEADO
                            Using con2 As SqlConnection = GetConnectionXML()
                                Try
                                    con2.Open()
                                    Dim cmd2 As New SqlCommand
                                    cmd2.Connection = con2
                                    cmd2.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CNPJ, RAZAO, CRITICA, TIPO) VALUES " +
                                        "('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', 'CMP', '" & sCnpjEmit & "', " +
                                        "'" & sNomeEmit & "', 'O FORNECEDOR ESTA INATIVO NO SISTEMA', 'C')"
                                    cmd2.ExecuteReader()
                                Catch ex2 As Exception
                                    Dim cmd2 As New SqlCommand
                                    cmd2.Connection = con2
                                    cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex2.Message & "')"
                                    cmd2.ExecuteReader()
                                    oReader.Close()
                                    con2.Dispose()
                                    OnElapsedTime(Me, e)
                                End Try
                                con2.Dispose()
                            End Using
                            bVal = False
                        End If
                    End If
                Else
                    Using con2 As SqlConnection = GetConnectionXML()
                        Try
                            con2.Open()
                            Dim cmd2 As New SqlCommand
                            cmd2.Connection = con2
                            cmd2.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CNPJ, RAZAO, CRITICA, TIPO) VALUES " +
                                "('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', 'CMP', '" & sCnpjEmit & "', " +
                                "'" & sNomeEmit & "', 'FORNECEDOR NAO CADASTRADO NO SISTEMA', 'C')"
                            cmd2.ExecuteReader()
                        Catch ex2 As Exception
                            Dim cmd2 As New SqlCommand
                            cmd2.Connection = con2
                            cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex2.Message & "')"
                            cmd2.ExecuteReader()
                            oReader.Close()
                            con2.Dispose()
                            OnElapsedTime(Me, e)
                        End Try
                        con2.Dispose()
                    End Using
                    bVal = False
                End If
                con.Dispose()
            Catch ex As Exception

            End Try
        End Using
        If sTpNf = "ENTRADA" Then
            Using con As SqlConnection = GetConnectionERP()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT A2_COND FROM SA2" & sCodTabEmitDest & "0 WHERE A2_COD = '" & sCodEmpEmit & "' AND D_E_L_E_T_ <> '*'"
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        If IsDBNull(dr.Item(0)) Or dr.Item(0).ToString.Replace(" ", "") = "" Then
                            Using con2 As SqlConnection = GetConnectionXML()
                                Try
                                    con2.Open()
                                    Dim cmd2 As New SqlCommand
                                    cmd2.Connection = con2
                                    cmd2.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CNPJ, RAZAO, CRITICA, TIPO) VALUES " +
                                        "('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', 'CMP', '" & sCnpjEmit & "', " +
                                        "'" & sNomeEmit & "', 'O FORNECEDOR NAO POSSUI COND. DE PAGTO CADASTRADA NO SISTEMA', 'C')"
                                    cmd2.ExecuteReader()
                                Catch ex2 As Exception
                                    Dim cmd2 As New SqlCommand
                                    cmd2.Connection = con2
                                    cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex2.Message & "')"
                                    cmd2.ExecuteReader()
                                    oReader.Close()
                                    con2.Dispose()
                                    OnElapsedTime(Me, e)
                                End Try
                                con2.Dispose()
                            End Using
                            bVal = False
                        End If
                    Else
                        Using con2 As SqlConnection = GetConnectionXML()
                            Try
                                con2.Open()
                                Dim cmd2 As New SqlCommand
                                cmd2.Connection = con2
                                cmd2.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CNPJ, RAZAO, CRITICA, TIPO) VALUES " +
                                    "('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', 'CMP', '" & sCnpjEmit & "', " +
                                    "'" & sNomeEmit & "', 'O FORNECEDOR NAO POSSUI COND. DE PAGTO CADASTRADA NO SISTEMA', 'C')"
                                cmd2.ExecuteReader()
                            Catch ex2 As Exception
                                Dim cmd2 As New SqlCommand
                                cmd2.Connection = con2
                                cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex2.Message & "')"
                                cmd2.ExecuteReader()
                                oReader.Close()
                                con2.Dispose()
                                OnElapsedTime(Me, e)
                            End Try
                            con2.Dispose()
                        End Using
                        bVal = False
                    End If
                    con.Dispose()
                Catch ex As Exception

                End Try
            End Using
        End If
        Return bVal
    End Function

    Sub GeraCabNf(ByVal caminho As String, ByVal xmlDoc As XmlDocument, ByVal e As System.EventArgs)
        fLog("Função", "GeraCabNf")
        Try
            'Cria uma instância de um documento XML
            Dim ns As New XmlNamespaceManager(xmlDoc.NameTable)
            ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe")
            Dim xpathNav As XPathNavigator = xmlDoc.CreateNavigator()
            Dim node As XPathNavigator
            Dim sLinha As String
            Dim sConvCnpj As String
            Dim i As Integer = 0
            Dim bSai As Boolean = False
            Dim sCodProd As String
            Dim bProdCad As Boolean = True
            Dim bValServico As Boolean = True
            Dim sCompCfop As String
            Dim iItemServ As Integer = 1
            Dim dAliqPis As Double = 0
            Dim dAliqCofins As Double = 0
            Dim dBCCofins As Double = 0
            Dim dBCPis As Double = 0
            'POPULANDO TABELA SF1XX0
            While Not bSai
                i = i + 1
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:xProd", ns)
                If node Is Nothing Then
                    bSai = True
                    i = i - 1
                Else
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:CFOP", ns)
                    sCompCfop = node.InnerXml.Substring(1, 3)
                    If Not sIssRegra.Contains(sCompCfop) Then
                        iItemServ = i
                        bValServico = False
                        'Exit While
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:PIS/nfe:PISAliq/nfe:pPIS", ns)
                    If Not node Is Nothing Then
                        dAliqPis = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dAliqPis = 0
                    End If
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:PIS/nfe:PISAliq/nfe:vBC", ns)
                    If Not node Is Nothing Then
                        dBCPis += node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dBCPis += 0
                    End If

                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:pCOFINS", ns)
                    If Not node Is Nothing Then
                        dAliqCofins = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dAliqCofins = 0
                    End If
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:vBC", ns)
                    If Not node Is Nothing Then
                        dBCCofins += node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dBCCofins += 0
                    End If
                End If
            End While
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & iItemServ & "]/nfe:prod/nfe:CFOP", ns)
            sCompCfop = node.InnerXml.Substring(1, 3)
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & iItemServ & "]/nfe:prod/nfe:cProd", ns)
            sCodProd = Strings.Left(node.InnerXml.ToString, 20)

            Dim sIdPrd As String = ""
            If sTpNf = "ENTRADA" Then
                sIdPrd = ""
                Using con As SqlConnection = GetConnectionERP() 'Verifica amarração Prod X Fornecedor
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT A5_PRODUTO FROM SA5" & sCodTabEmitDest & "0 WHERE A5_FORNECE = '" & sCodEmpEmitDest & "' AND A5_CODPRF = '" & sCodProd & "' AND D_E_L_E_T_ <> '*'"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            sIdPrd = dr.Item(0)
                        End If
                    Catch ex As Exception
                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = conError
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            conError.Dispose()
                            OnElapsedTime(Me, e)
                        End Using
                    End Try
                    con.Dispose()
                End Using
            ElseIf sTpNf = "TRANSF_ENTSAI" Or sTpNf = "TRANSF_ENT" Or sTpNf = "ENT_IMP" Or sTpNf = "SAIDA" Then
                sIdPrd = ""
                Using con As SqlConnection = GetConnectionERP() 'Verifica o cadastro do produto
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT B1_COD FROM SB1" & sCodTabEmitDest & "0 WHERE B1_COD = '" & sCodProd & "' AND B1_FILIAL = '" & sFilialEmitDest & "' AND D_E_L_E_T_ <> '*'"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            sIdPrd = dr.Item(0)
                        Else
                            bProdCad = False
                        End If
                    Catch ex As Exception
                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = conError
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            conError.Dispose()
                            OnElapsedTime(Me, e)
                        End Using
                    End Try
                    con.Dispose()
                End Using
            End If
            Dim sNumeroMov As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:nNF", ns)
            sNumeroMov = Strings.Right(Space(9).Replace(" ", "0") & node.InnerXml.ToString, 9) 'Numero do Movimento 
            Dim sSerie As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:serie", ns)
            sSerie = node.InnerXml.ToString      'Serie 
            Dim sIdNatMov As String
            Dim sCodnatMov As String
            Dim sIcms, sCst As String

            If iSitAtu = 1 Then
                sIcms = FPegaIcmsSn(xmlDoc, 1)
            ElseIf iSitAtu = 2 Or iSitAtu = 3 Then
                sIcms = FPegaIcms(xmlDoc, 1)
            End If
            If iSitAtu = 1 Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & iItemServ & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:CSOSN", ns)
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & iItemServ & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:CST", ns)
            End If
            If Not node Is Nothing Then
                sCst = node.InnerXml.ToString.Replace(".", ",")
            Else
                sCst = ""
            End If
            Dim matriz(), matrizRevenda(), matrizUsoConsu(), matrizAtivo() As String

            Dim j As Integer

            Dim bSimST As Boolean = False
            matriz = sCstRegra.Split(",")
            For j = 0 To matriz.GetUpperBound(0)
                If sCst = matriz(j) Then
                    bSimST = True
                End If
            Next

            Dim bValRevenda As Boolean = False
            matrizRevenda = sSitMercRevRegra.Split(",")
            For j = 0 To matrizRevenda.GetUpperBound(0)
                If sCompCfop = matrizRevenda(j) Then
                    bValRevenda = True
                End If
            Next

            Dim bValUsoConsu As Boolean = False
            matrizUsoConsu = sSitMercUsoRegra.Split(",")
            For j = 0 To matrizUsoConsu.GetUpperBound(0)
                If sCompCfop = matrizUsoConsu(j) Then
                    bValUsoConsu = True
                End If
            Next

            Dim bValAtivo As Boolean = False
            matrizAtivo = sSitMercAtivoRegra.Split(",")
            For j = 0 To matrizAtivo.GetUpperBound(0)
                If sCompCfop = matrizAtivo(j) Then
                    bValAtivo = True
                End If
            Next

            Dim sSitMerc As String = ""
            Dim sCSTPCfins As String = ""

            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & iItemServ & "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:CST", ns)
            If Not node Is Nothing Then
                sCSTPCfins = node.InnerXml.ToString
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & iItemServ & "]/nfe:imposto/nfe:COFINS/nfe:COFINSNT/nfe:CST", ns)
                If Not node Is Nothing Then
                    sCSTPCfins = node.InnerXml.ToString
                End If
            End If

            If sCompCfop = sCupomFiscalRegra Then
                sCompCfop = sOutrosMercServRegra
            End If

            Dim sCondPagto As String = "''"
            Using con As SqlConnection = GetConnectionERP()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT A2_COND FROM SA2" & sCodTabEmitDest & "0 WHERE A2_COD = '" & sCodEmpEmit & "' AND D_E_L_E_T_ <> '*'"
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        sCondPagto = "'" & dr.Item(0).ToString & "'"         'Codigo da condicao de pagto
                    End If
                    con.Dispose()
                Catch ex As Exception
                    Using conError As SqlConnection = GetConnectionXML()
                        conError.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = conError
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        conError.Dispose()
                        OnElapsedTime(Me, e)
                    End Using
                End Try
                con.Dispose()
            End Using

            Dim sValorBruto, sValorFrete As Decimal
            Dim dvProduto, dVst, dVipi, dSeg, dDesc, dOutro, dBaseIcms, dValIcms As Decimal
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vFrete", ns)
            sValorFrete = node.InnerXml.ToString.Replace(".", ",")
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vProd", ns)
            dvProduto = node.InnerXml.ToString.Replace(".", ",")
            sValorBruto = node.InnerXml.ToString.Replace(".", ",")
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vSeg", ns)
            dSeg = node.InnerXml.ToString.Replace(".", ",")
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vDesc", ns)
            dDesc = node.InnerXml.ToString.Replace(".", ",")
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vOutro", ns)
            dOutro = node.InnerXml.ToString.Replace(".", ",")
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vST", ns)
            dVst = node.InnerXml.ToString.Replace(".", ",")
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vIPI", ns)
            dVipi = node.InnerXml.ToString.Replace(".", ",")
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vBC", ns)
            dBaseIcms = node.InnerXml.ToString.Replace(".", ",")
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vICMS", ns)
            dValIcms = node.InnerXml.ToString.Replace(".", ",")
            Dim sVLiquid As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vNF", ns)
            sVLiquid = node.InnerXml.ToString.Replace(".", ",")
            Dim sPlaca As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:veicTransp/nfe:placa", ns)
            If node Is Nothing Then
                sPlaca = "''"
            Else
                sPlaca = "'" & node.InnerXml.ToString & "'"     'Placa Transportadora
            End If
            Dim sUFTransp As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:veicTransp/nfe:UF", ns)
            If node Is Nothing Then
                sUFTransp = "''"
            Else
                sUFTransp = "'" & node.InnerXml.ToString & "'"     'UF Placa Transportadora
            End If
            Dim sPesoLiq, sPesoBruto As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:vol/nfe:pesoL", ns)
            If node Is Nothing Then
                sPesoLiq = "'0'"
            Else
                sPesoLiq = "'" & node.InnerXml.ToString & "'"     'Peso liquido
            End If
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:vol/nfe:pesoB", ns)
            If node Is Nothing Then
                sPesoBruto = "'0'"
            Else
                sPesoBruto = "'" & node.InnerXml.ToString & "'"      'Peso bruto 
            End If
            Dim bSai2 As Boolean = False
            Dim i2 As Integer = 0
            Dim sEspecie As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:vol/nfe:esp", ns)
            If node Is Nothing Then
                sEspecie = ""
            Else
                sEspecie = node.InnerXml.ToString   'Especie
            End If
            Dim sDataLacto As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:dEmi", ns)
            sDataLacto = node.InnerXml.ToString      'Data de lancamento  
            Dim sDataMov As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:dSaiEnt", ns)
            If Not node Is Nothing Then
                sDataMov = "'" & node.InnerXml.ToString & "'"       'Data do movimento 
            Else
                sDataMov = "'" & sDataLacto & "'"          'Data do movimento 
            End If
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:dSaiEnt", ns)
            If Not node Is Nothing Then
                sDataSaida = "'" & node.InnerXml.ToString & "'"
            Else
                sDataSaida = "'" & sDataLacto & "'"
            End If
            Dim sModFrete As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:modFrete", ns)
            If Not node Is Nothing Then
                sModFrete = node.InnerXml.ToString
            Else
                sModFrete = "9"
            End If
            Dim sCnpjTransp, sQuantVol, sMarcaTransp As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:transporta/nfe:CNPJ", ns)
            If Not node Is Nothing Then
                sCnpjTransp = node.InnerXml.ToString
            Else
                sCnpjTransp = ""
            End If
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:vol/nfe:qVol", ns)
            If Not node Is Nothing Then
                sQuantVol = node.InnerXml.ToString
            Else
                sQuantVol = "0"
            End If
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:vol/nfe:marca", ns)
            If Not node Is Nothing Then
                sMarcaTransp = "'" & Strings.Left(node.InnerXml.ToString, 10) & "'"
            Else
                sMarcaTransp = "NULL"
            End If
            Dim sCodTRA As String = "NULL"
            If Not sCnpjTransp = "" Then
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con '"SELECT A4_COD FROM SA4" & sCodTabEmitDest & "0 WHERE A4_CGC = '" & sCnpjTransp & "' AND D_E_L_E_T_ <> '*'"
                        cmd.CommandText = "SELECT A4_COD FROM SA4" & sCodTabEmitDest & "0 WHERE A4_CGC = '" & sCnpjTransp & "' AND D_E_L_E_T_ <> '*'"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            If Not IsDBNull(dr.Item(0)) And dr.Item(0) <> "" Then
                                sCodTRA = dr.Item(0).ToString    'Cod. Transportadora
                            End If
                        Else
                            Dim sNomeTransp, sIETransp, sEnderTransp, sMunTransp As String
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:transporta/nfe:xNome", ns)
                            If Not node Is Nothing Then
                                sNomeTransp = Strings.Left(node.InnerXml.ToString, 40)
                            Else
                                sNomeTransp = ""
                            End If
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:transporta/nfe:IE", ns)
                            If Not node Is Nothing Then
                                sIETransp = node.InnerXml.ToString
                            Else
                                sIETransp = ""
                            End If
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:transporta/nfe:xEnder", ns)
                            If Not node Is Nothing Then
                                sEnderTransp = node.InnerXml.ToString
                            Else
                                sEnderTransp = ""
                            End If
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:transporta/nfe:xMun", ns)
                            If Not node Is Nothing Then
                                sMunTransp = node.InnerXml.ToString
                            Else
                                sMunTransp = ""
                            End If
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:transporta/nfe:UF", ns)
                            If Not node Is Nothing Then
                                sUFTransp = node.InnerXml.ToString
                            Else
                                sUFTransp = ""
                            End If
                            Using con2 As SqlConnection = GetConnectionERP()
                                Try
                                    con2.Open()
                                    Dim cmd2 As New SqlCommand
                                    cmd2.Connection = con2
                                    cmd2.CommandText = "SELECT MAX(A4_COD)+1 FROM SA4" & sCodTabEmitDest & "0 WHERE D_E_L_E_T_ <> '*'"
                                    Dim dr2 As SqlDataReader = cmd2.ExecuteReader()
                                    dr2.Read()
                                    If dr2.HasRows Then
                                        sCodTRA = Strings.Right(Space(6).Replace(" ", "0") & dr2.Item(0), 6) 'Numero do Movimento 
                                    End If
                                Catch ex As Exception
                                    Using conError As SqlConnection = GetConnectionXML()
                                        conError.Open()
                                        Dim cmd2 As New SqlCommand
                                        cmd2.Connection = conError
                                        cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                        cmd2.ExecuteReader()
                                        oReader.Close()
                                        conError.Dispose()
                                        OnElapsedTime(Me, e)
                                    End Using
                                End Try
                                con2.Dispose()
                            End Using
                            If sCodTRA = "000000" Then
                                sCodTRA = "000001"
                            End If True Then

End If
                        Using con2 As SqlConnection = GetConnectionERP()
                                Try
                                    con2.Open()
                                    Dim cmd2 As New SqlCommand
                                    cmd2.Connection = con2
                                    cmd2.CommandText = "INSERT INTO SA4" & sCodTabEmitDest & "0 (A4_COD,A4_NOME,A4_NREDUZ,A4_END,A4_MUN,A4_EST,A4_CGC,A4_INSEST) " +
                                        "VALUES ('" & sCodTRA & "','" & Strings.Left(sNomeTransp, 40) & "','" & Strings.Left(sNomeTransp, 15) & "','" & Strings.Left(sEnderTransp, 40) & "'," +
                                        "'" & Strings.Left(sMunTransp, 15) & "','" & sUFTransp & "','" & sCnpjTransp & "','" & Strings.Left(sIETransp, 15) & "')"
                                    cmd2.ExecuteReader()
                                Catch ex As Exception
                                    Using conError As SqlConnection = GetConnectionXML()
                                        conError.Open()
                                        Dim cmd2 As New SqlCommand
                                        cmd2.Connection = conError
                                        cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                        cmd2.ExecuteReader()
                                        oReader.Close()
                                        conError.Dispose()
                                        OnElapsedTime(Me, e)
                                    End Using
                                End Try
                                con2.Dispose()
                            End Using
                        End If
                    Catch ex As Exception
                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = conError
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            conError.Dispose()
                            OnElapsedTime(Me, e)
                        End Using
                    End Try
                    con.Dispose()
                End Using
            End If
            Dim sChaveNfe As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe", ns)
            sChaveNfe = Strings.Right(node.GetAttribute("Id", ""), 44)       'Chave de acesso da NF-e
            Dim sEspNf As String = ""
            If sTpNf = "TRANSF_ENT" Or sTpNf = "ENTRADA" Or sTpNf = "ENT_IMP" Then
                sEspNf = "NFE"
            Else
                sEspNf = "NF"
            End If
            If sEspNf = "NFE" Then
                Dim iRecnoSF1 As Integer = 0
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT MAX(R_E_C_N_O_)+1 FROM SF1" & sCodTabEmitDest & "0"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            If IsDBNull(dr.Item(0)) Then
                                iRecnoSF1 = 1
                            Else
                                iRecnoSF1 = dr.Item(0)
                            End If
                        End If
                    Catch ex As Exception
                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = conError
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            conError.Dispose()
                            OnElapsedTime(Me, e)
                        End Using
                    End Try
                    con.Dispose()
                End Using
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO SF1" & sCodTabEmitDest & "0 (F1_FILIAL,F1_DOC,F1_SERIE,F1_FORNECE,F1_LOJA,F1_COND,F1_DUPL,F1_EMISSAO,F1_EST,F1_FRETE,F1_DESPESA," +
                            "F1_BASEICM,F1_VALICM,F1_BASEIPI,F1_VALIPI,F1_VALMERC,F1_VALBRUT,F1_TIPO,F1_DESCONT,F1_DTDIGIT,F1_ORIGLAN,F1_CONTSOC,F1_IRRF,F1_ESPECIE,F1_II,F1_BASIMP5," +
                            "F1_BASIMP6,F1_VALIMP5,F1_VALIMP6,F1_SEGURO,F1_MOEDA,F1_PREFIXO,F1_STATUS,F1_RECBMTO,F1_RECISS,R_E_C_N_O_,F1_CHVNFE) " +
                            "VALUES ('" & sFilialEmitDest & "','" & sNumeroMov & "','" & sSerie & "','" & sCodEmpEmit & "','" & sLojaEmit & "'," & sCondPagto & ",'" & sNumeroMov & "'," +
                            "'" & sDataEmissao.Replace("-", "") & "','" & sUFEmit & "','" & Str(sValorFrete) & "','" & Str(dOutro) & "', '" & Str(dBaseIcms) & "','" & Str(dValIcms) & "'," +
                            "'" & IIf(dVipi > 0, Str(sValorBruto + dOutro + sValorFrete + dSeg), 0) & "','" & Str(dVipi) & "','" & Str(dvProduto) & "','" & Str(sValorBruto) & "','N','" & Str(dDesc) & "','" & Format(Date.Now, "yyyyMMdd") & "','PX'," +
                            "0,0,'" & sEspNf & "',0,'" & Str(dBCCofins) & "','" & Str(dBCPis) & "','" & Str(Round(dBCCofins * (dAliqCofins / 100), 2)) & "','" & Str(Round(dBCPis * (dAliqPis / 100), 2)) & "'," +
                            "'" & Str(dSeg) & "',1,1,'','" & Format(Date.Now, "yyyyMMdd") & "','2','" & iRecnoSF1 & "','" & sChaveNfe & "')"
                        cmd.ExecuteReader()
                    Catch ex As Exception
                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = conError
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            conError.Dispose()
                            OnElapsedTime(Me, e)
                        End Using
                    End Try
                    con.Dispose()
                End Using
            Else
                Dim iRecnoSF2 As Integer = 0
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT MAX(R_E_C_N_O_)+1 FROM SF2" & sCodTabEmitDest & "0"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            If IsDBNull(dr.Item(0)) Then
                                iRecnoSF2 = 1
                            Else
                                iRecnoSF2 = dr.Item(0)
                            End If
                        End If
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using

                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO SF2" & sCodTabEmitDest & "0 (F2_FILIAL,F2_DOC,F2_SERIE,F2_CLIENTE,F2_CLIENT,F2_LOJA,F2_LOJENT,F2_COND,F2_DUPL,F2_EMISSAO,F2_EST,F2_FRETE,F2_DESPESA," +
                            "F2_BASEICM,F2_VALICM,F2_BASEIPI,F2_VALIPI,F2_VALMERC,F2_VALBRUT,F2_TIPO,F2_DESCONT,F2_DTDIGIT,F2_CONTSOC,F2_ESPECIE,F2_BASIMP5," +
                            "F2_BASIMP6,F2_VALIMP5,F2_VALIMP6,F2_SEGURO,F2_MOEDA,F2_PREFIXO,F2_RECISS,R_E_C_N_O_,F2_ICMFRET,F2_TIPOCLI,F2_VOLUME1,F2_VOLUME2,F2_VOLUME3,F2_VOLUME4," +
                            "F2_ICMSRET,F2_PLIQUI, F2_PBRUTO,F2_HORA,F2_RECFAUT,F2_CHVNFE) " +
                            "VALUES ('" & sFilialEmitDest & "','" & sNumeroMov & "','" & sSerie & "','" & sCodEmpDest & "','" & sCodEmpDest & "','" & sLojaDest & "','" & sLojaDest & "'," & sCondPagto & ",'" & sNumeroMov & "'," +
                            "'" & sDataEmissao.Replace("-", "") & "','" & sUFDest & "','" & Str(sValorFrete) & "','" & Str(dOutro) & "', '" & Str(dBaseIcms) & "','" & Str(dValIcms) & "'," +
                            "'" & IIf(dVipi > 0, Str(sValorBruto + dOutro + sValorFrete + dSeg), 0) & "','" & Str(dVipi) & "','" & Str(dvProduto) & "','" & Str(sValorBruto) & "','N','" & Str(dDesc) & "','" & Format(Date.Now, "yyyyMMdd") & "'," +
                            "0,'" & sEspNf & "','" & Str(dBCCofins) & "','" & Str(dBCPis) & "','" & Str(Round(dBCCofins * (dAliqCofins / 100), 2)) & "','" & Str(Round(dBCPis * (dAliqPis / 100), 2)) & "'," +
                            "'" & Str(dSeg) & "',1,1,'2','" & iRecnoSF2 & "',0,'" & sTipoCli & "','" & sQuantVol & "',0,0,0,0," & sPesoLiq & "," & sPesoBruto & ",'" & FormatDateTime(Date.Now, DateFormat.ShortTime) & "', " +
                            "'1','" & sChaveNfe & "')"
                        cmd.ExecuteReader()
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using

                'CRIA CABECALHO DO PEDIDO
                Dim iRecnoSC5 As Integer = 0
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT MAX(R_E_C_N_O_)+1 FROM SC5" & sCodTabEmitDest & "0"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            If IsDBNull(dr.Item(0)) Then
                                iRecnoSC5 = 1
                            Else
                                iRecnoSC5 = dr.Item(0)
                            End If
                        End If
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using
                sSC5Num = ""
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT MAX(C5_NUM)+1 FROM SC5" & sCodTabEmitDest & "0 WHERE C5_FILIAL = '" & sFilialEmitDest & "'"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            If IsDBNull(dr.Item(0)) Then
                                sSC5Num = "000001"
                            Else
                                sSC5Num = Strings.Right(Space(6).Replace(" ", "0") & dr.Item(0).ToString, 6)
                            End If
                        End If
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO SC5" & sCodTabEmitDest & "0 (C5_FILIAL,C5_NUM,C5_TIPO,C5_CLIENTE,C5_CLIENT,C5_LOJACLI,C5_LOJAENT,C5_TIPOCLI,C5_CONDPAG,C5_EMISSAO,C5_FRETE,C5_SEGURO,C5_DESPESA," +
                            "C5_MOEDA,C5_PESOL,C5_PBRUTO,C5_LIBEROK,C5_NOTA,C5_SERIE,C5_TIPLIB,C5_DESCONT,C5_TXMOEDA,C5_TPCARGA,C5_GERAWMS,C5_SOLOPC,R_E_C_N_O_,R_E_C_D_E_L_) " +
                            "VALUES ('" & sFilialEmitDest & "','" & sSC5Num & "','N','" & sCodEmpDest & "','" & sCodEmpDest & "','" & sLojaDest & "','" & sLojaDest & "','" & sTipoCli & "'," & sCondPagto & "," +
                            "'" & sDataEmissao.Replace("-", "") & "','" & Str(sValorFrete) & "','" & Str(dSeg) & "','" & Str(dOutro) & "',1," & sPesoLiq & "," & sPesoBruto & ",'S','" & sNumeroMov & "'," +
                            "'" & sSerie & "','1','" & Str(dDesc) & "','1','2','1','1','" & iRecnoSC5 & "',0)"
                        cmd.ExecuteReader()
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using
            End If
        Catch
        End Try
    End Sub

    Sub GeraItensNf(ByVal caminho As String, ByVal xmlDoc As XmlDocument, ByVal i As Integer, ByVal e As System.EventArgs)

        Try
            'Cria uma instância de um documento XML
            Dim ns As New XmlNamespaceManager(xmlDoc.NameTable)
            ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe")
            Dim xpathNav As XPathNavigator = xmlDoc.CreateNavigator()
            Dim node As XPathNavigator
            Dim sCodProd, sIdPrd As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:cProd", ns)
            sCodProd = Strings.Left(node.InnerXml.ToString, 20)
            sIdPrd = ""
            If sTpNf = "ENTRADA" Then
                Using con As SqlConnection = GetConnectionERP() 'Verifica amarração Prod X Fornecedor
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT A5_PRODUTO FROM SA5" & sCodTabEmitDest & "0 WHERE A5_FORNECE = '" & sCodEmpEmitDest & "' AND A5_CODPRF = '" & sCodProd & "' AND D_E_L_E_T_ <> '*'"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            sIdPrd = dr.Item(0).ToString.Replace(" ", "")
                        End If
                    Catch ex As Exception
                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = conError
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            conError.Dispose()
                            OnElapsedTime(Me, e)
                        End Using
                    End Try
                    con.Dispose()
                End Using
            Else
                Using con As SqlConnection = GetConnectionERP() 'Verifica o cadastro do produto
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT B1_COD FROM SB1" & sCodTabEmitDest & "0 WHERE B1_COD = '" & sCodProd & "' AND B1_FILIAL = '" & sFilialEmitDest & "' AND D_E_L_E_T_ <> '*'"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            sIdPrd = dr.Item(0)
                        End If
                    Catch ex As Exception
                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = conError
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            conError.Dispose()
                            OnElapsedTime(Me, e)
                        End Using
                    End Try
                    con.Dispose()
                End Using
            End If
            Dim sTipoProd As String = ""
            Dim sNomePrd As String = ""
            Using con As SqlConnection = GetConnectionERP() 'Verifica o cadastro do produto
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT B1_TIPO, B1_DESC FROM SB1" & sCodTabEmitDest & "0 WHERE B1_COD = '" & sIdPrd & "' AND B1_FILIAL = '" & sFilialEmitDest & "' AND D_E_L_E_T_ <> '*'"
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        sTipoProd = dr.Item(0)
                        sNomePrd = dr.Item(1).ToString
                    End If
                Catch ex As Exception
                    Using conError As SqlConnection = GetConnectionXML()
                        conError.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = conError
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        conError.Dispose()
                        OnElapsedTime(Me, e)
                    End Using
                End Try
                con.Dispose()
            End Using
            Dim sQuant, svUnit As String
            Dim dQuant, dResult As Double
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:qCom", ns)
            sQuant = node.InnerXml.ToString     'Quantidade
            dQuant = node.InnerXml.ToString     'Quantidade
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vUnCom", ns)
            svUnit = node.InnerXml.ToString     'Preco unitario
            Dim sCodUnd As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:uCom", ns)
            sCodUnd = node.InnerXml.ToString     'Unidade de Medida
            Dim bValUndSist As Boolean = True
            Using con As SqlConnection = GetConnectionXML()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT UNDMED FROM UNDMED WHERE UNDMEDGB = '" & sCodUnd & "' "
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        sCodUnd = dr.Item(0).ToString
                    End If
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con.Dispose()
            End Using
            Dim sIcms, sCst As String
            Dim sIdNatItem As String
            Dim sCodnatItem, sUFEmit As String
            Dim sCfop As String
            Dim dAliqIcms, dVIcms, dBc, dIpi, dValIpi, bRedBC, bRedBCST, dValProd, dValFrete, dValDesc, dValOutro, dValSeg, dMvaST, dVIcmsSt, dBcST, dAliqIcmsSt As Double
            Dim dValTNFiscal, dValTNf, dValTPrd, dValTICMSST, dValTFrete, dValTSeguro, dValTDesc, dValTOutraDesp, dValTIPI As Double
            Dim dValTNfCalc, dValTICMSSTCalc As Double
            Dim iIdRegraIcms As Integer
            Dim sCompCfop As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:CFOP", ns)
            sCompCfop = node.InnerXml.Substring(1, 3)
            Dim bSimST As Boolean
            If iSitAtu = 1 Then
                sIcms = FPegaIcmsSn(xmlDoc, i)
            ElseIf iSitAtu = 2 Or iSitAtu = 3 Then
                sIcms = FPegaIcms(xmlDoc, i)
            End If
            If iSitAtu = 1 Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:CSOSN", ns)
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:CST", ns)
            End If
            If Not node Is Nothing Then
                sCst = node.InnerXml.ToString.Replace(".", ",")
            Else
                sCst = ""
            End If
            bSimST = False

            Dim sGrpTrib As String = ""
            Using con As SqlConnection = GetConnectionERP() 'Verifica o cadastro do produto
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT B1_GRTRIB FROM SB1" & sCodTabEmitDest & "0 WHERE B1_COD = '" & sIdPrd & "' AND B1_FILIAL = '" & sFilialEmitDest & "' AND D_E_L_E_T_ <> '*'"
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        sGrpTrib = dr.Item(0).ToString.Replace(" ", "")
                    End If
                Catch ex As Exception
                    Using conError As SqlConnection = GetConnectionXML()
                        conError.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = conError
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        conError.Dispose()
                        OnElapsedTime(Me, e)
                    End Using
                End Try
                con.Dispose()
            End Using
            Dim sTESEnt As String = ""
            Dim sTipoEntSai As String = ""

            If sTpNf = "TRANSF_ENTSAI" Or sTpNf = "SAIDA" Then
                sTipoEntSai = "B1.B1_TS"
            Else
                sTipoEntSai = "B1.B1_TE"
            End If

            Using con As SqlConnection = GetConnectionERP()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT SUBSTRING(F4.F4_CF,2,3) AS COMPCFOP, F4.F4_CODIGO FROM SF4" & sCodTabEmitDest & "0 F4 INNER JOIN SB1" & sCodTabEmitDest & "0 B1 ON F4.F4_CODIGO = " & sTipoEntSai & " WHERE " +
                        "B1.B1_COD = '" & sIdPrd & "' AND B1.B1_FILIAL = '" & sFilialEmitDest & "' AND F4.F4_FILIAL = '" & sFilialEmitDest & "' AND B1.D_E_L_E_T_ <> '*' AND F4.D_E_L_E_T_ <> '*'"
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        sCompCfop = dr.Item(0).ToString
                        sTESEnt = dr.Item(1).ToString
                    End If
                Catch ex As Exception
                    Using conError As SqlConnection = GetConnectionXML()
                        conError.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = conError
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()

                        conError.Dispose()
                        OnElapsedTime(Me, e)
                    End Using
                End Try
                con.Dispose()
            End Using

            sCfop = sDFEst & sCompCfop

            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vProd", ns)
            If Not node Is Nothing Then
                dValProd = node.InnerXml.ToString.Replace(".", ",")
            Else
                dValProd = 0
            End If
            dValTPrd += Round(dValProd, 2)
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vFrete", ns)
            If Not node Is Nothing Then
                dValFrete = node.InnerXml.ToString.Replace(".", ",")
            Else
                dValFrete = 0
            End If
            dValTFrete += Round(dValFrete, 2)
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vDesc", ns)
            If Not node Is Nothing Then
                dValDesc = node.InnerXml.ToString.Replace(".", ",")
            Else
                dValDesc = 0
            End If
            dValTDesc += Round(dValDesc, 2)
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vOutro", ns)
            If Not node Is Nothing Then
                dValOutro = node.InnerXml.ToString.Replace(".", ",")
            Else
                dValOutro = 0
            End If
            dValTOutraDesp += Round(dValOutro, 2)
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vSeg", ns)
            If Not node Is Nothing Then
                dValSeg = node.InnerXml.ToString.Replace(".", ",")
            Else
                dValSeg = 0
            End If
            dValTSeguro += Round(dValSeg, 2)
            If iSitAtu = 1 Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pCredSN", ns)
            Else

            End If
            If Not node Is Nothing Then
                dAliqIcms = node.InnerXml.ToString.Replace(".", ",")
            Else
                dAliqIcms = 0
            End If
            If iSitAtu = 1 Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:vCredICMSSN", ns)
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:vICMS", ns)
            End If
            If Not node Is Nothing Then
                dVIcms = node.InnerXml.ToString.Replace(".", ",")
            Else
                dVIcms = 0
            End If
            If iSitAtu = 1 Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:vBC", ns)
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:vBC", ns)
            End If
            If Not node Is Nothing Then
                dBc = node.InnerXml.ToString.Replace(".", ",")
            Else
                dBc = 0
            End If
            If iSitAtu = 1 Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pRedBC", ns)
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pRedBC", ns)
            End If
            If Not node Is Nothing Then
                bRedBC = node.InnerXml.ToString.Replace(".", ",")
            Else
                bRedBC = 0
            End If
            If iSitAtu = 1 Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pRedBCST", ns)
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pRedBCST", ns)
            End If
            If Not node Is Nothing Then
                bRedBCST = node.InnerXml.ToString.Replace(".", ",")
            Else
                bRedBCST = 0
            End If
            If iSitAtu = 1 Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pMVAST", ns)
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pMVAST", ns)
            End If
            If Not node Is Nothing Then
                dMvaST = node.InnerXml.ToString.Replace(".", ",")
            Else
                dMvaST = 0
            End If
            If iSitAtu = 1 Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:vBCST", ns)
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:vBCST", ns)
            End If
            If Not node Is Nothing Then
                dBcST = node.InnerXml.ToString.Replace(".", ",")
            Else
                dBcST = 0
            End If
            If iSitAtu = 1 Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:vICMSST", ns)
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:vICMSST", ns)
            End If
            If Not node Is Nothing Then
                dVIcmsSt = node.InnerXml.ToString.Replace(".", ",")
            Else
                dVIcmsSt = 0
            End If

            Dim sUfDest As String = ""
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:dest/nfe:enderDest/nfe:UF", ns)
            If Not node Is Nothing Then
                sUfDest = node.InnerXml.ToString
            Else
                sUfDest = ""
            End If

            If iSitAtu = 1 Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:pICMSST", ns)
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:pICMSST", ns)
            End If
            If Not node Is Nothing Then
                dAliqIcmsSt = node.InnerXml.ToString.Replace(".", ",")
            Else
                dAliqIcmsSt = 0
            End If

            If iSitAtu = 1 Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:CSOSN", ns)
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:CST", ns)
            End If
            If Not node Is Nothing Then
                sCst = node.InnerXml.ToString.Replace(".", ",")
            Else
                sCst = ""
            End If
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:IPI/nfe:IPITrib/nfe:pIPI", ns)
            If Not node Is Nothing Then
                dIpi = node.InnerXml.ToString.Replace(".", ",")
            Else
                dIpi = 0
            End If
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:IPI/nfe:IPITrib/nfe:vIPI", ns)
            If Not node Is Nothing Then
                dValIpi = node.InnerXml.ToString.Replace(".", ",")
            Else
                dValIpi = 0
            End If


            Dim dVPrecoPauta, dVAliqPauta, dBcSTPauta As Double
            Dim dBcCalc, dValIcmsCalc, dBcSTCalc, dValIcmsSTCalc, dFatorMva As Double
            Using con4 As SqlConnection = GetConnectionERP()
                Try
                    con4.Open()
                    Dim cmd4 As New SqlCommand
                    cmd4.Connection = con4
                    cmd4.CommandText = "SELECT F7_MARGEM, F7_VLR_ICM, F7_VLRICMP FROM SF7" & sCodTabEmitDest & "0 WHERE F7_GRTRIB = '" & sGrpTrib & "' AND F7_FILIAL = '" & sFilialEmitDest & "' AND F7_EST = '" & sUFEmitDest & "' AND D_E_L_E_T_ <> '*'"
                    Dim dr4 As SqlDataReader = cmd4.ExecuteReader()
                    dr4.Read()
                    If dr4.HasRows Then
                        If IsDBNull(dr4.Item(0)) Then
                            dFatorMva = 0
                            dVPrecoPauta = 0
                            dVAliqPauta = 0
                        Else
                            dFatorMva = dr4.Item(0)
                            dVAliqPauta = dr4.Item(1)
                            dVPrecoPauta = dr4.Item(2)
                        End If
                    Else
                        dFatorMva = 0
                        dVPrecoPauta = 0
                        dVAliqPauta = 0
                    End If
                Catch ex As Exception
                    Using conError As SqlConnection = GetConnectionXML()
                        conError.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = conError
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        conError.Dispose()
                        OnElapsedTime(Me, e)
                    End Using
                End Try
                con4.Dispose()
            End Using

            Dim sVTotItem As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:vProd", ns)
            sVTotItem = node.InnerXml.ToString      'Valor total do item
            Dim sCodTrib As String
            If iSitAtu = 1 Then
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" & sIcms & "/nfe:orig", ns)
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:orig", ns)
            End If
            If Not node Is Nothing Then
                sCodTrib = node.InnerXml.ToString
            Else
                sCodTrib = ""
            End If
            Dim sIcmsSnxLr As String = ""
            Dim sIcmsTemp As String = ""
            Select Case sCst
                Case "101"
                    sIcmsSnxLr = "00"
                Case "102"
                    sIcmsSnxLr = "41"
                Case "103"
                    sIcmsSnxLr = "40"
                Case "201"
                    sIcmsSnxLr = "10"
                Case "202"
                    sIcmsSnxLr = "30"
                Case "203"
                    sIcmsSnxLr = "30"
                Case "300"
                    sIcmsSnxLr = "40"
                Case "400"
                    sIcmsSnxLr = "41"
                Case "500"
                    sIcmsSnxLr = "60"
                Case "900"
                    sIcmsSnxLr = "90"
            End Select
            If Not sIcmsSnxLr = "" Then
                sIcmsTemp = sCst
                sCst = sIcmsSnxLr
            End If
            If iSitAtu = 1 Then
                sCodTrib = sCodTrib & sIcmsSnxLr
            Else
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ICMS/nfe:ICMS" & sIcms & "/nfe:CST", ns)
                If Not node Is Nothing Then
                    sCodTrib = sCodTrib & node.InnerXml.ToString
                Else
                    sCodTrib = ""
                End If
            End If
            If iSitAtu = 1 Then
                sCst = sIcmsTemp
            End If
            Dim sMunicipio As String
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:ISSQN/nfe:cMunFG", ns)
            If Not node Is Nothing Then
                sMunicipio = "'" & node.InnerXml.ToString & "'"
            Else
                sMunicipio = "NULL"
            End If

            Dim sNumDoc As String = ""
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:nNF", ns)
            sNumDoc = Strings.Right(Space(9).Replace(" ", "0") & node.InnerXml.ToString, 9) 'Numero do Movimento 
            Dim sSerie As String = ""
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:serie", ns)
            sSerie = node.InnerXml.ToString      'Serie

            Dim dAliqPis, dBCPis, dValPis As Double
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:PIS/nfe:PISAliq/nfe:pPIS", ns)
            If Not node Is Nothing Then
                dAliqPis = node.InnerXml.ToString.Replace(".", ",")
            Else
                dAliqPis = 0
            End If
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:PIS/nfe:PISAliq/nfe:vBC", ns)
            If Not node Is Nothing Then
                dBCPis = node.InnerXml.ToString.Replace(".", ",")
            Else
                dBCPis = 0
            End If
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:PIS/nfe:PISAliq/nfe:vPIS", ns)
            If Not node Is Nothing Then
                dValPis = node.InnerXml.ToString.Replace(".", ",")
            Else
                dValPis = 0
            End If

            Dim dAliqCofins, dBCCofins, dValCofins As Double
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:pCOFINS", ns)
            If Not node Is Nothing Then
                dAliqCofins = node.InnerXml.ToString.Replace(".", ",")
            Else
                dAliqCofins = 0
            End If
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:vBC", ns)
            If Not node Is Nothing Then
                dBCCofins = node.InnerXml.ToString.Replace(".", ",")
            Else
                dBCCofins = 0
            End If
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:vCOFINS", ns)
            If Not node Is Nothing Then
                dValCofins = node.InnerXml.ToString.Replace(".", ",")
            Else
                dValCofins = 0
            End If
            Dim iRecno As Integer = 0

            If sTpNf = "TRANSF_ENT" Or sTpNf = "ENTRADA" Or sTpNf = "ENT_IMP" Then
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT MAX(R_E_C_N_O_)+1 FROM SD1" & sCodTabEmitDest & "0"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            If IsDBNull(dr.Item(0)) Then
                                iRecno = 1
                            Else
                                iRecno = dr.Item(0)
                            End If
                        End If
                    Catch ex As Exception
                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = conError
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            conError.Dispose()
                            OnElapsedTime(Me, e)
                        End Using
                    End Try
                    con.Dispose()
                End Using
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO SD1" & sCodTabEmitDest & "0 (D1_FILIAL,D1_ITEM,D1_COD,D1_UM,D1_QUANT,D1_VUNIT,D1_TOTAL" +
                        ",D1_VALIPI,D1_VALICM,D1_TES,D1_CF,D1_DESC,D1_IPI, D1_PICM, D1_FORNECE, D1_LOJA,D1_LOCAL,D1_DOC,D1_EMISSAO,D1_DTDIGIT,D1_TIPO,D1_SERIE,D1_TP,D1_BASEICM," +
                                    "D1_VALDESC,D1_BASEIPI,D1_BASIMP5,D1_BASIMP6,D1_VALIMP5,D1_VALIMP6,D1_ALQIMP5,D1_ALQIMP6,D1_VALFRE,D1_SEGURO,D1_DESPESA,D1_STSERV," +
                                    "D1_ALIQSOL,D1_GARANTI,D1_ALQCSL,D1_ALQPIS,D1_ALQCOF,R_E_C_N_O_) " +
                        "VALUES ('" & sFilialEmitDest & "','" & Strings.Right(Space(4).Replace(" ", "0") & i, 4) & "','" & sIdPrd & "','" & sCodUnd & "','" & sQuant.Replace(" ", "") & "','" & svUnit.Replace(" ", "") & "'," +
                        "'" & Str(dValProd).Replace(" ", "") & "','" & Str(dValIpi).Replace(" ", "") & "','" & Str(dVIcms).Replace(" ", "") & "','" & sTESEnt & "','" & sCfop & "','" & Str(dValDesc).Replace(" ", "") & "'," +
                        "'" & Str(dIpi).Replace(" ", "") & "','" & Str(dAliqIcms).Replace(" ", "") & "', '" & sCodEmpEmit & "','" & sLojaEmit & "','" & sLojaEmit & "','" & sNumDoc & "','" & sDataEmissao.Replace("-", "") & "'," +
                        "'" & Format(Date.Now, "yyyyMMdd") & "', 'N','" & sSerie & "', '" & sTipoProd & "', '" & Str(dBc).Replace(" ", "") & "', '" & Str(dValDesc).Replace(" ", "") & "'," +
                        "'" & IIf(dIpi > 0, Str(dValProd + dValFrete + dValSeg - dValDesc).Replace(" ", ""), 0) & "','" & Str(dBCCofins).Replace(" ", "") & "','" & Str(dBCPis).Replace(" ", "") & "','" & Str(dValCofins).Replace(" ", "") & "'," +
                        "'" & Str(dValPis) & "','" & Str(dAliqCofins) & "','" & Str(dAliqPis) & "','" & Str(dValFrete) & "','" & Str(dValDesc) & "','" & Str(dValOutro) & "','1','" & Str(dAliqIcms) & "','N','1'," +
                        "'" & Str(dAliqPis) & "','" & Str(dAliqCofins) & "'," & iRecno & ")"
                        cmd.ExecuteReader()
                    Catch ex As Exception
                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = conError
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            OnElapsedTime(Me, e)
                        End Using
                    End Try
                    con.Dispose()
                End Using
            Else
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT MAX(R_E_C_N_O_)+1 FROM SD2" & sCodTabEmitDest & "0"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            If IsDBNull(dr.Item(0)) Then
                                iRecno = 1
                            Else
                                iRecno = dr.Item(0)
                            End If
                        End If
                    Catch ex As Exception
                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = conError
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            conError.Dispose()
                            OnElapsedTime(Me, e)
                        End Using
                    End Try
                    con.Dispose()
                End Using

                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO SD2" & sCodTabEmitDest & "0 (D2_FILIAL,D2_ITEM,D2_COD,D2_UM,D2_QUANT,D2_PRCVEN,D2_PRUNIT,D2_TOTAL,D2_VALBRUT," +
                        "D2_VALIPI,D2_VALICM,D2_TES,D2_CF,D2_DESC,D2_IPI, D2_PICM, D2_CLIENTE, D2_LOJA,D2_LOCAL,D2_DOC,D2_EMISSAO,D2_DTDIGIT,D2_TIPO,D2_SERIE,D2_TP,D2_BASEICM," +
                        "D2_BASEIPI,D2_BASIMP5,D2_BASIMP6,D2_VALIMP5,D2_VALIMP6,D2_ALQIMP5,D2_ALQIMP6,D2_VALFRE,D2_SEGURO,D2_DESPESA,D2_STSERV," +
                        "D2_ALIQSOL,D2_ALQCSL,D2_ALQPIS,D2_ALQCOF,R_E_C_N_O_,D2_CLASFIS) " +
                        "VALUES ('" & sFilialEmitDest & "','" & Strings.Right(Space(2).Replace(" ", "0") & i, 2) & "','" & sIdPrd & "','" & sCodUnd & "','" & sQuant.Replace(" ", "") & "','" & svUnit.Replace(" ", "") & "','" & svUnit.Replace(" ", "") & "'," +
                        "'" & Str(dValProd).Replace(" ", "") & "','" & Str(dValProd).Replace(" ", "") & "','" & Str(dValIpi).Replace(" ", "") & "','" & Str(dVIcms).Replace(" ", "") & "','" & sTESEnt & "','" & sCfop & "','" & Str(dValDesc).Replace(" ", "") & "'," +
                        "'" & Str(dIpi).Replace(" ", "") & "','" & Str(dAliqIcms).Replace(" ", "") & "', '" & sCodEmpEmit & "','" & sLojaEmit & "','" & sLojaEmit & "','" & sNumDoc & "','" & sDataEmissao.Replace("-", "") & "'," +
                        "'" & Format(Date.Now, "yyyyMMdd") & "', 'N','" & sSerie & "', '" & sTipoProd & "', '" & Str(dBc).Replace(" ", "") & "'," +
                        "'" & IIf(dIpi > 0, Str(dValProd + dValFrete + dValSeg - dValDesc).Replace(" ", ""), 0) & "','" & Str(dBCCofins).Replace(" ", "") & "','" & Str(dBCPis).Replace(" ", "") & "','" & Str(dValCofins).Replace(" ", "") & "'," +
                        "'" & Str(dValPis) & "','" & Str(dAliqCofins) & "','" & Str(dAliqPis) & "','" & Str(dValFrete) & "','" & Str(dValDesc) & "','" & Str(dValOutro) & "','1','" & Str(dAliqIcms) & "','1'," +
                        "'" & Str(dAliqPis) & "','" & Str(dAliqCofins) & "'," & iRecno & ",'" & sCodTrib & "')"
                        cmd.ExecuteReader()
                    Catch ex As Exception
                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = conError
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            conError.Dispose()
                            OnElapsedTime(Me, e)
                        End Using
                    End Try
                    con.Dispose()
                End Using
                'CRIA LIBERCAO DO PEDIDO
                Dim iRecnoSC9 As Integer = 0
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT MAX(R_E_C_N_O_)+1 FROM SC9" & sCodTabEmitDest & "0"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            If IsDBNull(dr.Item(0)) Then
                                iRecnoSC9 = 1
                            Else
                                iRecnoSC9 = dr.Item(0)
                            End If
                        End If
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using
                Dim sNumSeqC9 As String = ""
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT MAX(C9_NUMSEQ)+1 FROM SC9" & sCodTabEmitDest & "0"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            If IsDBNull(dr.Item(0)) Then
                                sNumSeqC9 = "000001"
                            Else
                                sNumSeqC9 = Strings.Right(Space(6).Replace(" ", "0") & dr.Item(0).ToString, 6)
                            End If
                        End If
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using

                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO SC9" & sCodTabEmitDest & "0 (C9_OK,C9_FILIAL,C9_PEDIDO,C9_ITEM,C9_CLIENTE,C9_LOJA,C9_PRODUTO,C9_QTDLIB,C9_NFISCAL,C9_SERIENF,C9_DATALIB,C9_SEQUEN," +
                            "C9_PRCVEN,C9_BLEST,C9_BLCRED,C9_LOCAL,C9_TPCARGA,C9_NUMSEQ,C9_RETOPER,R_E_C_N_O_,R_E_C_D_E_L_) " +
                            "VALUES ('" & Strings.Right(Space(4).Replace(" ", "0") & sNumDoc, 4) & "','" & sFilialEmitDest & "','" & sSC5Num & "','" & Strings.Right(Space(2).Replace(" ", "0") & i, 2) & "', " +
                            "'" & sCodEmpDest & "','" & sLojaDest & "','" & sIdPrd & "','" & sQuant.Replace(" ", "") & "','" & sNumDoc & "','" & sSerie & "','" & sDataEmissao.Replace("-", "") & "','01', " +
                            "'" & svUnit.Replace(" ", "") & "','10','10','01','2','" & sNumSeqC9 & "','2','" & iRecnoSC9 & "',0)"
                        cmd.ExecuteReader()
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using
                Dim iRecnoSC6 As Integer = 0
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT MAX(R_E_C_N_O_)+1 FROM SC6" & sCodTabEmitDest & "0"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            If IsDBNull(dr.Item(0)) Then
                                iRecnoSC6 = 1
                            Else
                                iRecnoSC6 = dr.Item(0)
                            End If
                        End If
                    Catch ex As Exception
                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = conError
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            conError.Dispose()
                            OnElapsedTime(Me, e)
                        End Using
                    End Try
                    con.Dispose()
                End Using
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO SC6" & sCodTabEmitDest & "0 (C6_FILIAL,C6_ITEM,C6_PRODUTO,C6_UM,C6_QTDVEN,C6_PRCVEN,C6_VALOR,C6_TES,C6_LOCAL,C6_CF,C6_QTDENT,C6_CLI,C6_ENTREG,C6_LOJA," +
                        "C6_NOTA,C6_SERIE,C6_DATFAT,C6_NUM,C6_DESCRI,C6_PRUNIT,C6_CLASFIS,C6_TPOP,C6_SUGENTR,C6_RATEIO,R_E_C_N_O_,R_E_C_D_E_L_) VALUES ('" & sFilialEmitDest & "'," +
                        "'" & Strings.Right(Space(2).Replace(" ", "0") & i, 2) & "','" & sIdPrd.Replace(" ", "") & "','" & sCodUnd & "','" & sQuant.Replace(" ", "") & "','" & svUnit.Replace(" ", "") & "'," +
                        "'" & Str(dValProd).Replace(" ", "") & "','" & sTESEnt & "','01','" & sCfop & "','" & sQuant.Replace(" ", "") & "','" & sCodEmpEmit & "','" & sDataEmissao.Replace("-", "") & "'," +
                        "'" & sLojaEmit & "','" & sNumDoc & "','" & sSerie & "','" & sDataEmissao.Replace("-", "") & "', '" & sSC5Num & "','" & sNomePrd & "','" & svUnit.Replace(" ", "") & "'," +
                        "'" & sCodTrib & "','F','" & sDataEmissao.Replace("-", "") & "','2'," & iRecnoSC6 & ",'0')"
                        cmd.ExecuteReader()
                    Catch ex As Exception
                        Using conError As SqlConnection = GetConnectionXML()
                            conError.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = conError
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            conError.Dispose()
                            OnElapsedTime(Me, e)
                        End Using
                    End Try
                    con.Dispose()
                End Using
            End If
        Catch
        End Try
    End Sub

    Function FPegaIcms(ByVal xmlDoc As XmlDocument, ByVal item As Integer) As String

        Dim ns As New XmlNamespaceManager(xmlDoc.NameTable)
        ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe")
        Dim xpathNav As XPathNavigator = xmlDoc.CreateNavigator()
        Dim node00 As XPathNavigator
        Dim node10 As XPathNavigator
        Dim node20 As XPathNavigator
        Dim node30 As XPathNavigator
        Dim node40 As XPathNavigator
        Dim node41 As XPathNavigator
        Dim node50 As XPathNavigator
        Dim node51 As XPathNavigator
        Dim node60 As XPathNavigator
        Dim node70 As XPathNavigator
        Dim node90 As XPathNavigator
        Dim sResp As String = ""

        node00 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMS00/nfe:orig", ns)
        node10 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMS10/nfe:orig", ns)
        node20 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMS20/nfe:orig", ns)
        node30 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMS30/nfe:orig", ns)
        node40 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMS40/nfe:orig", ns)
        node41 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMS41/nfe:orig", ns)
        node50 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMS50/nfe:orig", ns)
        node51 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMS51/nfe:orig", ns)
        node60 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMS60/nfe:orig", ns)
        node70 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMS70/nfe:orig", ns)
        node90 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMS90/nfe:orig", ns)

        If Not node00 Is Nothing Then
            sResp = "00"
        End If
        If Not node10 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "10", sResp & ";10")
        End If
        If Not node20 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "20", sResp & ";20")
        End If
        If Not node30 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "30", sResp & ";30")
        End If
        If Not node40 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "40", sResp & ";40")
        End If
        If Not node41 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "41", sResp & ";41")
        End If
        If Not node50 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "50", sResp & ";50")
        End If
        If Not node51 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "51", sResp & ";51")
        End If
        If Not node60 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "60", sResp & ";60")
        End If
        If Not node70 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "70", sResp & ";70")
        End If
        If Not node90 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "90", sResp & ";90")
        End If
        Return sResp
    End Function

    Function FPegaIcmsSn(ByVal xmlDoc As XmlDocument, ByVal item As Integer) As String
        Dim ns As New XmlNamespaceManager(xmlDoc.NameTable)
        ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe")
        Dim xpathNav As XPathNavigator = xmlDoc.CreateNavigator()
        Dim node101 As XPathNavigator
        Dim node102 As XPathNavigator
        Dim node103 As XPathNavigator
        Dim node201 As XPathNavigator
        Dim node202 As XPathNavigator
        Dim node203 As XPathNavigator
        Dim node300 As XPathNavigator
        Dim node400 As XPathNavigator
        Dim node500 As XPathNavigator
        Dim node900 As XPathNavigator
        Dim sResp As String = ""

        node101 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN101/nfe:orig", ns)
        node102 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN102/nfe:orig", ns)
        node103 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN103/nfe:orig", ns)
        node201 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN201/nfe:orig", ns)
        node202 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN202/nfe:orig", ns)
        node203 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN203/nfe:orig", ns)
        node300 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN300/nfe:orig", ns)
        node400 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN400/nfe:orig", ns)
        node500 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN500/nfe:orig", ns)
        node900 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & item & "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN900/nfe:orig", ns)

        If Not node101 Is Nothing Then
            sResp = "101"
        End If
        If Not node102 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "102", sResp & ";102")
        End If
        If Not node103 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "103", sResp & ";103")
        End If
        If Not node201 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "201", sResp & ";201")
        End If
        If Not node202 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "202", sResp & ";202")
        End If
        If Not node203 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "203", sResp & ";203")
        End If
        If Not node300 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "300", sResp & ";300")
        End If
        If Not node400 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "400", sResp & ";400")
        End If
        If Not node500 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "500", sResp & ";500")
        End If
        If Not node900 Is Nothing Then
            sResp = IIf(sResp.Trim = "", "900", sResp & ";900")
        End If
        Return sResp
    End Function

    Public Function fRemoverAcentos(ByVal Valor As String) As String

        If bAtivaLog Then
            fLog(arq.Name, "Função fRemoverAcentos")
        End If

        Dim TextoNormalizado As String = Valor.Normalize(NormalizationForm.FormD)
        Dim sbTexto As New StringBuilder

        For i = 0 To TextoNormalizado.Length - 1
            Dim C As Char = TextoNormalizado(i)
            If C = "'" Then
                C = "`"
            End If
            If (CharUnicodeInfo.GetUnicodeCategory(C) <> UnicodeCategory.NonSpacingMark) Then
                sbTexto.Append(C)
            End If
        Next
        Return sbTexto.ToString()
    End Function

    Private Sub EnviaEmailFiscal(ByVal sArqXmlMail As String, ByVal e As System.EventArgs)

        If bAtivaEmail = False Then
            Exit Sub
        End If

        If bAtivaLog Then
            fLog(arq.Name, "Função EnviaEmailFiscal")
        End If

        Dim iEmailPorXML As Integer
        Using con As SqlConnection = GetConnectionXML()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT ATIVO FROM REGRASXML WHERE TIPO_VALIDACAO = 'EMAILPORXML'"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                dr.Read()
                If dr.HasRows Then
                    iEmailPorXML = dr.Item(0) * -1
                End If
            Catch ex As Exception
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                oReader.Close()
                con.Dispose()
                OnElapsedTime(Me, e)
            End Try
            con.Dispose()
        End Using

        If iEmailPorXML = 1 Then
            Dim sTipoEmail As String
            If sTpNf = "ENTRADA" Or sTpNf = "TRANSF_ENT" Then
                sTipoEmail = "ENTRADA"
            Else
                sTipoEmail = "SAIDA"
            End If
            Dim bValXml As Boolean = True
            Using con As SqlConnection = GetConnectionXML()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    Dim dr As SqlDataReader
                    cmd.CommandText = "SELECT DISTINCT (FLAG_STATUS) FROM CRITICAXML WHERE SETOR = 'FIS' AND FLAG_STATUS IS NULL AND NOME_XML = '" & sArqXmlMail & "'"
                    dr = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        If IsDBNull(dr.Item(0)) Then
                            bValXml = True
                        End If
                    End If
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con.Dispose()
            End Using

            If bValXml Then
                Dim sConta, sSenha, sParaFiscal, sAssunto, sSmtp, sPorta, sBody, sTable As String
                sConta = configurationAppSettings.GetValue("Conta", GetType(System.String))
                sSenha = configurationAppSettings.GetValue("Senha", GetType(System.String))
                sParaFiscal = configurationAppSettings.GetValue("ParaFiscal", GetType(System.String))
                sAssunto = configurationAppSettings.GetValue("Assunto", GetType(System.String))
                sSmtp = configurationAppSettings.GetValue("Smtp", GetType(System.String))
                sPorta = configurationAppSettings.GetValue("Porta", GetType(System.String))
                Dim smtpServer As New SmtpClient()
                Dim mail As New MailMessage
                smtpServer.Credentials = New Net.NetworkCredential(sConta, sSenha)
                smtpServer.Port = sPorta
                smtpServer.Host = sSmtp
                smtpServer.EnableSsl = False
                mail.From = New MailAddress(sParaFiscal)
                mail.IsBodyHtml = True
                Dim sArqXml As String
                Using con As SqlConnection = GetConnectionXML()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT NOME_XML, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, CRITICA, VALOR_PRD, CST, ALIQ_ICMS, " +
                            "ALIQ_ICMSST, RED_BC, VALOR_ICMS_XML, VALOR_ICMS_CALC, VALOR_ICMSST_XML, VALOR_ICMSST_CALC, VALOR_MVA_XML, " +
                            "VALOR_MVA_PRD, VALOR_BCST_XML, VALOR_BCST_CALC, VALOR_BCRED_XML, VALOR_BCRED_CALC FROM CRITICAXML " +
                            "WHERE SETOR = 'FIS' AND NOME_XML = '" & sArqXmlMail & "' AND FLAG_STATUS IS NULL"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        If dr.HasRows Then
                            While dr.Read()
                                sArqXml = dr.Item(0)
                                sTable += "	<tr bgcolor='#FFFFFF'>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(1) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(2) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(3) & "</span></div></td>"
                                sTable += "		<td width='360'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(4) & "</span></div></td>"
                                sTable += "		<td width='360'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(5) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(6) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(7) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(8) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(9) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(10) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(11) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(12) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(13) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(14) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(15) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(16) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(17) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(18) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(19) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(20) & "</span></div></td>"
                                sTable += "	</tr>"
                            End While
                        End If
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using
                sBody += "<!DOCTYPE HTML PUBLIC '-//W3C//DTD HTML 4.01 Transitional//EN'"
                sBody += "'http://www.w3.org/TR/html4/loose.dtd'>"
                sBody += "<html>"
                sBody += "<head>"
                sBody += "<meta http-equiv='Content-Type' content='text/html; charset=iso-8859-1'>"
                sBody += "<title>Untitled Document</title>"
                sBody += "<style type='text/css'>"
                sBody += "<!--"
                sBody += ".style1 {"
                sBody += "	font-family: Verdana, Arial, Helvetica, sans-serif;"
                sBody += "	font-size: 14px;"
                sBody += "	color: #003f75;"
                sBody += "}"
                sBody += "body {"
                sBody += "	background-color: #FFFFFF;"
                sBody += "}"
                sBody += ".style2 {"
                sBody += "	font-family: Verdana, Arial, Helvetica, sans-serif;"
                sBody += "	font-weight: bold; font-size: 12px"
                sBody += "}"
                sBody += ".style8 {color: #FFFFFF}"
                sBody += ".style10 {font-family: Verdana, Arial, Helvetica, sans-serif; font-weight: normal; font-size: 10px; color: #000000; }"
                sBody += "-->"
                sBody += "</style>"
                sBody += "</head>"
                sBody += "<body>"
                sBody += "<div align='center'><img src='" & sLogo & "' width='85' height='85'>"
                sBody += "</div>"
                sBody += "<H1 align='center' class='style1'>XML " & sTipoEmail & "</H1>"
                sBody += "<blockquote>"
                sBody += "</blockquote>"
                sBody += "<div align='center'>"
                sBody += "  <blockquote>"
                sBody += "    <p><span class='style2'>DIVERGENCIAS NO CONTEUDO DO XML " & sArqXml & "<br>"
                sBody += "      Segue abaixo a relação das divergências encontradas. </span>"
                sBody += "    </p>"
                sBody += "  </blockquote>"
                sBody += "</div>"
                sBody += "<table width='1300' align='center' bgcolor='#003f75'>"
                sBody += "	<tr bgcolor='#003f75'>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>ITEM</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "		<div align='center' class='style8'><span class='style2'>PRD TOTVS</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>PRD FOR</span></div></td>"
                sBody += "		<td width='360'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>DESC PRODUTO</span></div></td>"
                sBody += "		<td width='360'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>CRITICA</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>VALOR PRD</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>CST</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>ALIQ ICMS</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>ALIQ ICMSST</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>RED BC</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>ICMS XML</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>ICMS CALC</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>ICMSST XML</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>ICMSST CALC</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>MVA XML</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>MVA PRD</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>BCST XML</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>BCST CALC</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>BCRED XML</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>BCRED CALC</span></div></td>"
                sBody += "	</tr>"
                sBody += sTable
                sBody += "</table>"
                sBody += "<br>"
                sBody += "<div align='justify' class='style10'>Esta mensagem é confidencial, destinada exclusivamente ao(s) seu(s) destinatário(s), estando protegida pelo sigilo previsto na legislação. A sua divulgação, distribuição ou reprodução indevida sujeita o infrator às sanções criminais e cíveis. Caso tenha recebido esta mensagem indevidamente, favor devolvê-la imediatamente ao remetente."
                sBody += "This message is confidential, exclusively destinated to the addressee(s), being it´s secrecy protected by law. Divulgation, distribution or unnappropriated reproduction subject the transgressor to criminal and civil sanctions. If you have received this message in error, please return it immediately to the sender."
                sBody += "</div>"
                sBody += "</body>"
                sBody += "</html>"
                mail.To.Add(sParaFiscal)
                mail.Subject = sAssunto & sTipoEmail
                mail.Body = sBody
                smtpServer.Send(mail)
                mail.Dispose()
            End If
        End If
    End Sub

    Private Sub EnviaEmailCad(ByVal sArqXmlMail As String, ByVal e As System.EventArgs)

        If bAtivaEmail = False Then
            Exit Sub
        End If

        If bAtivaLog Then
            fLog(arq.Name, "Função EnviaEmailcad")
        End If

        Dim iEmailPorXML As Integer
        Using con As SqlConnection = GetConnectionXML()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT ATIVO FROM REGRASXML WHERE TIPO_VALIDACAO = 'EMAILPORXML'"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                dr.Read()
                If dr.HasRows Then
                    iEmailPorXML = dr.Item(0) * -1
                End If
            Catch ex As Exception
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                oReader.Close()
                con.Dispose()
                OnElapsedTime(Me, e)
            End Try
            con.Dispose()
        End Using
        If iEmailPorXML = 1 Then
            Dim sTipoEmail As String
            If sTpNf = "ENTRADA" Or sTpNf = "TRANSF_ENT" Then
                sTipoEmail = "ENTRADA"
            Else
                sTipoEmail = "SAIDA"
            End If
            Dim bValXml As Boolean = False
            Using con As SqlConnection = GetConnectionXML()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    Dim dr As SqlDataReader
                    cmd.CommandText = "SELECT DISTINCT (FLAG_STATUS) FROM CRITICAXML WHERE SETOR = 'CMP' AND FLAG_STATUS IS NULL AND NOME_XML = '" & sArqXmlMail & "'"
                    dr = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        If IsDBNull(dr.Item(0)) Then
                            bValXml = True
                        End If
                    End If
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con.Dispose()
            End Using
            If bValXml Then
                Dim sConta, sSenha, sParaCadastro, sAssunto, sSmtp, sPorta, sBody, sTable As String
                sConta = configurationAppSettings.GetValue("Conta", GetType(System.String))
                sSenha = configurationAppSettings.GetValue("Senha", GetType(System.String))
                sParaCadastro = configurationAppSettings.GetValue("ParaCadastro", GetType(System.String))
                sAssunto = configurationAppSettings.GetValue("Assunto", GetType(System.String))
                sSmtp = configurationAppSettings.GetValue("Smtp", GetType(System.String))
                sPorta = configurationAppSettings.GetValue("Porta", GetType(System.String))
                Dim smtpServer As New SmtpClient()
                Dim mail As New MailMessage
                smtpServer.Credentials = New Net.NetworkCredential(sConta, sSenha)
                smtpServer.Port = sPorta
                smtpServer.Host = sSmtp
                smtpServer.EnableSsl = False
                mail.From = New MailAddress(sParaCadastro)
                mail.IsBodyHtml = True
                Dim sArqXml As String
                Using con As SqlConnection = GetConnectionXML()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT NOME_XML, ITEM_XML, COD_PRD, COD_PRD_AUX, DESC_PRD, CNPJ, RAZAO, CRITICA " +
                            "FROM CRITICAXML WHERE SETOR = 'CMP' AND NOME_XML = '" & sArqXmlMail & "' AND FLAG_STATUS IS NULL"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        If dr.HasRows Then
                            While dr.Read()
                                sArqXml = dr.Item(0)
                                sTable += "	<tr bgcolor='#FFFFFF'>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(1) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(2) & "</span></div></td>"
                                sTable += "		<td width='40'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(3) & "</span></div></td>"
                                sTable += "		<td width='360'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(4) & "</span></div></td>"
                                sTable += "		<td width='120'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(5) & "</span></div></td>"
                                sTable += "		<td width='300'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(6) & "</span></div></td>"
                                sTable += "		<td width='360'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(7) & "</span></div></td>"
                                sTable += "	</tr>"
                            End While
                        End If
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using
                sBody += "<!DOCTYPE HTML PUBLIC '-//W3C//DTD HTML 4.01 Transitional//EN'"
                sBody += "'http://www.w3.org/TR/html4/loose.dtd'>"
                sBody += "<html>"
                sBody += "<head>"
                sBody += "<meta http-equiv='Content-Type' content='text/html; charset=iso-8859-1'>"
                sBody += "<title>Untitled Document</title>"
                sBody += "<style type='text/css'>"
                sBody += "<!--"
                sBody += ".style1 {"
                sBody += "	font-family: Verdana, Arial, Helvetica, sans-serif;"
                sBody += "	font-size: 14px;"
                sBody += "	color: #003f75;"
                sBody += "}"
                sBody += "body {"
                sBody += "	background-color: #FFFFFF;"
                sBody += "}"
                sBody += ".style2 {"
                sBody += "	font-family: Verdana, Arial, Helvetica, sans-serif;"
                sBody += "	font-weight: bold; font-size: 12px"
                sBody += "}"
                sBody += ".style8 {color: #FFFFFF}"
                sBody += ".style10 {font-family: Verdana, Arial, Helvetica, sans-serif; font-weight: normal; font-size: 10px; color: #000000; }"
                sBody += "-->"
                sBody += "</style>"
                sBody += "</head>"
                sBody += "<body>"
                sBody += "<div align='center'><img src='" & sLogo & "' width='85' height='85'>"
                sBody += "</div>"
                sBody += "<H1 align='center' class='style1'>XML " & sTipoEmail & "</H1>"
                sBody += "<blockquote>"
                sBody += "</blockquote>"
                sBody += "<div align='center'>"
                sBody += "  <blockquote>"
                sBody += "    <p><span class='style2'>Divergências no conteúdo do XML " & sArqXml & "<br>"
                sBody += "      Segue abaixo a relação das divergências encontradas. </span>"
                sBody += "    </p>"
                sBody += "  </blockquote>"
                sBody += "</div>"
                sBody += "<table width='1300' align='center' bgcolor='#003f75'>"
                sBody += "	<tr bgcolor='#003f75'>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>ITEM</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "		<div align='center' class='style8'><span class='style2'>PRD RM</span></div></td>"
                sBody += "		<td width='40'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>PRD FOR</span></div></td>"
                sBody += "		<td width='360'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>DESC PRODUTO</span></div></td>"
                sBody += "		<td width='120'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>CNPJ</span></div></td>"
                sBody += "		<td width='300'>"
                sBody += "		<div align='center' class='style8'><span class='style2'>RAZAO SOCIAL</span></div></td>"
                sBody += "		<td width='360'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>CRITICA</span></div></td>"
                sBody += "	</tr>"
                sBody += sTable
                sBody += "</table>"
                sBody += "<br>"
                sBody += "<div align='justify' class='style10'>Esta mensagem é confidencial, destinada exclusivamente ao(s) seu(s) destinatário(s), estando protegida pelo sigilo previsto na legislação. A sua divulgação, distribuição ou reprodução indevida sujeita o infrator às sanções criminais e cíveis. Caso tenha recebido esta mensagem indevidamente, favor devolvê-la imediatamente ao remetente."
                sBody += "This message is confidential, exclusively destinated to the addressee(s), being it´s secrecy protected by law. Divulgation, distribution or unnappropriated reproduction subject the transgressor to criminal and civil sanctions. If you have received this message in error, please return it immediately to the sender."
                sBody += "</div>"
                sBody += "</body>"
                sBody += "</html>"
                mail.To.Add(sParaCadastro)
                mail.Subject = sAssunto & sTipoEmail
                mail.Body = sBody
                smtpServer.Send(mail)
                mail.Dispose()
            End If
        End If
    End Sub

    Private Sub EnviaEmailDiario(ByVal sEmailDiario As String, ByVal e As System.EventArgs)

        If bAtivaEmail = False Then
            Exit Sub
        End If

        If bAtivaLog Then
            fLog(arq.Name, "Função EnviaEmailDiario")
        End If

        Dim bValXml As Boolean = False
        Using con As SqlConnection = GetConnectionXML()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                Dim dr As SqlDataReader
                cmd.CommandText = "SELECT DISTINCT (NOME_XML) FROM CRITICAXML"
                dr = cmd.ExecuteReader()
                dr.Read()
                If dr.HasRows Then
                    If Not IsDBNull(dr.Item(0)) Then
                        bValXml = True
                    End If
                End If
            Catch ex As Exception
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                oReader.Close()
                con.Dispose()
                OnElapsedTime(Me, e)
            End Try
            con.Dispose()
        End Using
        If bValXml Then
            Dim sConta, sSenha, sParaCadastro, sAssunto, sSmtp, sPorta, sBody, sTable As String
            sConta = configurationAppSettings.GetValue("Conta", GetType(System.String))
            sSenha = configurationAppSettings.GetValue("Senha", GetType(System.String))
            sParaCadastro = configurationAppSettings.GetValue("ParaCadastro", GetType(System.String))
            sAssunto = configurationAppSettings.GetValue("Assunto", GetType(System.String))
            sSmtp = configurationAppSettings.GetValue("Smtp", GetType(System.String))
            sPorta = configurationAppSettings.GetValue("Porta", GetType(System.String))
            Dim smtpServer As New SmtpClient()
            Dim mail As New MailMessage
            smtpServer.Credentials = New Net.NetworkCredential(sConta, sSenha)
            smtpServer.Port = sPorta
            smtpServer.Host = sSmtp
            smtpServer.EnableSsl = False
            mail.From = New MailAddress(sParaCadastro)
            mail.IsBodyHtml = True
            Dim sArqXml As String
            Using con As SqlConnection = GetConnectionXML()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT DISTINCT NOME_XML, CNPJ, RAZAO, CRITICA FROM CRITICAXML"
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    If dr.HasRows Then
                        While dr.Read()
                            sTable += "	<tr bgcolor='#FFFFFF'>"
                            sTable += "		<td width='100'>"
                            sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(0) & "</span></div></td>"
                            sTable += "		<td width='120'>"
                            sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(1) & "</span></div></td>"
                            sTable += "		<td width='300'>"
                            sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(2) & "</span></div></td>"
                            sTable += "		<td width='360'>"
                            sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(3) & "</span></div></td>"
                            sTable += "	</tr>"
                        End While
                    End If
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con.Dispose()
            End Using
            sBody += "<!DOCTYPE HTML PUBLIC '-//W3C//DTD HTML 4.01 Transitional//EN'"
            sBody += "'http://www.w3.org/TR/html4/loose.dtd'>"
            sBody += "<html>"
            sBody += "<head>"
            sBody += "<meta http-equiv='Content-Type' content='text/html; charset=iso-8859-1'>"
            sBody += "<title>Untitled Document</title>"
            sBody += "<style type='text/css'>"
            sBody += "<!--"
            sBody += ".style1 {"
            sBody += "	font-family: Verdana, Arial, Helvetica, sans-serif;"
            sBody += "	font-size: 14px;"
            sBody += "	color: #003f75;"
            sBody += "}"
            sBody += "body {"
            sBody += "	background-color: #FFFFFF;"
            sBody += "}"
            sBody += ".style2 {"
            sBody += "	font-family: Verdana, Arial, Helvetica, sans-serif;"
            sBody += "	font-weight: bold; font-size: 12px"
            sBody += "}"
            sBody += ".style8 {color: #FFFFFF}"
            sBody += ".style10 {font-family: Verdana, Arial, Helvetica, sans-serif; font-weight: normal; font-size: 10px; color: #000000; }"
            sBody += "-->"
            sBody += "</style>"
            sBody += "</head>"
            sBody += "<body>"
            sBody += "<div align='center'><img src='" & sLogo & "' width='85' height='85'>"
            sBody += "</div>"
            sBody += "<H1 align='center' class='style1'>XML DIARIO</H1>"
            sBody += "<blockquote>"
            sBody += "</blockquote>"
            sBody += "<div align='center'>"
            sBody += "  <blockquote>"
            sBody += "    <p><span class='style2'>Divergências nos conteúdo dos XML's<br>"
            sBody += "      Segue abaixo a relação dos XML's com divergências. </span>"
            sBody += "    </p>"
            sBody += "  </blockquote>"
            sBody += "</div>"
            sBody += "<table width='1300' align='center' bgcolor='#003f75'>"
            sBody += "	<tr bgcolor='#003f75'>"
            sBody += "		<td width='100'>"
            sBody += "			<div align='center' class='style8'><span class='style2'>XML</span></div></td>"
            sBody += "		<td width='120'>"
            sBody += "			<div align='center' class='style8'><span class='style2'>CNPJ</span></div></td>"
            sBody += "		<td width='300'>"
            sBody += "		<div align='center' class='style8'><span class='style2'>RAZAO SOCIAL</span></div></td>"
            sBody += "		<td width='360'>"
            sBody += "			<div align='center' class='style8'><span class='style2'>CRITICA</span></div></td>"
            sBody += "	</tr>"
            sBody += sTable
            sBody += "</table>"
            sBody += "<br>"
            sBody += "<div align='justify' class='style10'>Esta mensagem é confidencial, destinada exclusivamente ao(s) seu(s) destinatário(s), estando protegida pelo sigilo previsto na legislação. A sua divulgação, distribuição ou reprodução indevida sujeita o infrator às sanções criminais e cíveis. Caso tenha recebido esta mensagem indevidamente, favor devolvê-la imediatamente ao remetente."
            sBody += "This message is confidential, exclusively destinated to the addressee(s), being it´s secrecy protected by law. Divulgation, distribution or unnappropriated reproduction subject the transgressor to criminal and civil sanctions. If you have received this message in error, please return it immediately to the sender."
            sBody += "</div>"
            sBody += "</body>"
            sBody += "</html>"
            mail.To.Add(sParaCadastro)
            mail.Subject = sAssunto & "DIARIO"
            mail.Body = sBody
            smtpServer.Send(mail)
            mail.Dispose()
            Using con As SqlConnection = GetConnectionXML() 'Setar Variavel para envio diario
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    Dim dr As SqlDataReader
                    cmd.CommandText = "UPDATE ZPARAMETROSLOG SET ATIVO = 1 WHERE TIPO_VALIDACAO = '" & sEmailDiario & "'"
                    dr = cmd.ExecuteReader()
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con.Dispose()
            End Using
        End If
    End Sub

    Private Sub EnviaEmailCompras(ByVal sArqXmlMail As String, ByVal e As System.EventArgs)

        If bAtivaEmail = False Then
            Exit Sub
        End If

        If bAtivaLog Then
            fLog(arq.Name, "Função EnviaEmailCompras")
        End If

        Dim iEmailPorXML As Integer
        Using con As SqlConnection = GetConnectionXML()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT ATIVO FROM ZPARAMETROSLOG WHERE TIPO_VALIDACAO = 'EMAILPORXML'"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                dr.Read()
                If dr.HasRows Then
                    iEmailPorXML = dr.Item(0) * -1
                End If
            Catch ex As Exception
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                oReader.Close()
                con.Dispose()
                OnElapsedTime(Me, e)
            End Try
            con.Dispose()
        End Using
        If iEmailPorXML = 1 Then
            Dim sTipoEmail As String
            If sTpNf = "ENTRADA" Or sTpNf = "TRANSF_ENT" Then
                sTipoEmail = "ENTRADA"
            Else
                sTipoEmail = "SAIDA"
            End If
            Dim bValXml As Boolean = False
            Using con As SqlConnection = GetConnectionXML()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    Dim dr As SqlDataReader
                    cmd.CommandText = "SELECT DISTINCT (FLAG_STATUS) FROM CRITICAXML WHERE SETOR = 'COMPRAS' AND FLAG_STATUS IS NULL AND NOME_XML = '" & sArqXmlMail & "'"
                    dr = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        If IsDBNull(dr.Item(0)) Then
                            bValXml = True
                        End If
                    End If
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con.Dispose()
            End Using
            If bValXml Then
                Dim sConta, sSenha, sParaCadastro, sAssunto, sSmtp, sPorta, sBody, sTable As String
                sConta = configurationAppSettings.GetValue("Conta", GetType(System.String))
                sSenha = configurationAppSettings.GetValue("Senha", GetType(System.String))
                sParaCadastro = configurationAppSettings.GetValue("ParaPo", GetType(System.String))
                sAssunto = configurationAppSettings.GetValue("Assunto", GetType(System.String))
                sSmtp = configurationAppSettings.GetValue("Smtp", GetType(System.String))
                sPorta = configurationAppSettings.GetValue("Porta", GetType(System.String))
                Dim smtpServer As New SmtpClient()
                Dim mail As New MailMessage
                smtpServer.Credentials = New Net.NetworkCredential(sConta, sSenha)
                smtpServer.Port = sPorta
                smtpServer.Host = sSmtp
                smtpServer.EnableSsl = False
                mail.From = New MailAddress(sParaCadastro)
                mail.IsBodyHtml = True
                Dim sArqXml As String
                Dim sNomeFor As String = ""
                Dim sCodEmpEmit As String = ""
                Dim sNf As String = ""
                Dim sSerie As String = ""
                Dim sCnpj As String = ""
                Dim sRazao As String = ""
                Dim bVal As Boolean = True
                Using con As SqlConnection = GetConnectionXML()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT NOME_XML, CNPJ, RAZAO, CODFOR, NUMEROMOV, SERIE, PEDIDO, ITEM_XML, COD_PRD, COD_PRD_AUX, SKU, DESC_PRD, QUANTIDADE, VALOR_PRD, CRITICA " +
                            "FROM CRITICAXML WHERE SETOR = 'COMPRAS' AND NOME_XML = '" & sArqXmlMail & "' AND FLAG_STATUS IS NULL"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        If dr.HasRows Then
                            While dr.Read()
                                If bVal Then
                                    sArqXml = dr.Item(0).ToString
                                    sCnpj = dr.Item(1).ToString
                                    sRazao = dr.Item(2).ToString
                                    sCodEmpEmit = dr.Item(3).ToString
                                    sNf = dr.Item(4).ToString
                                    sSerie = dr.Item(5).ToString
                                    bVal = False
                                End If
                                sTable += "	<tr bgcolor='#FFFFFF'>"
                                sTable += "		<td width='60'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(6) & "</span></div></td>" 'PEDIDO
                                sTable += "		<td width='50'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(7) & "</span></div></td>" 'ITEM
                                sTable += "		<td width='60'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(8) & "</span></div></td>" ' COD RM
                                sTable += "		<td width='60'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(9) & "</span></div></td>" 'COD FOR
                                sTable += "		<td width='60'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(10) & "</span></div></td>" 'SKU
                                sTable += "		<td width='300'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(11) & "</span></div></td>" 'DESC PRD
                                sTable += "		<td width='50'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(12) & "</span></div></td>" 'QUANT
                                sTable += "		<td width='60'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(13) & "</span></div></td>" 'VALOR PRD
                                sTable += "		<td width='360'>"
                                sTable += "			<div align='center' class='style8'><span class='style10'>" & dr.Item(14) & "</span></div></td>" 'CRITICA
                                sTable += "	</tr>"
                            End While
                        End If
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using
                Using con As SqlConnection = GetConnectionXML()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "SELECT NOME FROM FCFO WHERE CODCFO = '" & sCodEmpEmit & "'"
                        Dim dr As SqlDataReader = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            sNomeFor = dr.Item(0).ToString
                        End If
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using
                sBody += "<!DOCTYPE HTML PUBLIC '-//W3C//DTD HTML 4.01 Transitional//EN'"
                sBody += "'http://www.w3.org/TR/html4/loose.dtd'>"
                sBody += "<html>"
                sBody += "<head>"
                sBody += "<meta http-equiv='Content-Type' content='text/html; charset=iso-8859-1'>"
                sBody += "<title>Untitled Document</title>"
                sBody += "<style type='text/css'>"
                sBody += "<!--"
                sBody += ".style1 {"
                sBody += "	font-family: Verdana, Arial, Helvetica, sans-serif;"
                sBody += "	font-size: 14px;"
                sBody += "	color: #003f75;"
                sBody += "}"
                sBody += "body {"
                sBody += "	background-color: #FFFFFF;"
                sBody += "}"
                sBody += ".style2 {"
                sBody += "	font-family: Verdana, Arial, Helvetica, sans-serif;"
                sBody += "	font-weight: bold; font-size: 12px"
                sBody += "}"
                sBody += ".style8 {color: #FFFFFF}"
                sBody += ".style10 {font-family: Verdana, Arial, Helvetica, sans-serif; font-weight: normal; font-size: 10px; color: #000000; }"
                sBody += "-->"
                sBody += "</style>"
                sBody += "</head>"
                sBody += "<body>"
                sBody += "<div align='center'><img src='" & sLogo & "' width='85' height='85'>"
                sBody += "</div>"
                sBody += "<H1 align='center' class='style1'>XML " & sTipoEmail & "</H1>"
                sBody += "<blockquote>"
                sBody += "</blockquote>"
                sBody += "<div align='center'>"
                sBody += "  <blockquote>"
                sBody += "    <p><span class='style2'>Divergências no conteúdo do XML " & sArqXml & "<br>"
                sBody += "      Segue abaixo a relação das divergências encontradas. </span>"
                sBody += "    </p>"
                sBody += "  </blockquote>"
                sBody += "</div>"
                sBody += "<div align='center'>"
                sBody += "  <blockquote>"
                sBody += "    <p><span class='style2'>Fornecedor: " & sNomeFor & "</span>"
                sBody += "    </p>"
                sBody += "  </blockquote>"
                sBody += "</div>"
                sBody += "<div align='center'>"
                sBody += "  <blockquote>"
                sBody += "    <p><span class='style2'>NF N.: " & sNf & " / " & sSerie & "</span>"
                sBody += "    </p>"
                sBody += "  </blockquote>"
                sBody += "</div>"
                sBody += "<table width='1300' align='center' bgcolor='#003f75'>"
                sBody += "	<tr bgcolor='#003f75'>"
                sBody += "		<td width='60'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>PEDIDO</span></div></td>"
                sBody += "		<td width='50'>"
                sBody += "		<div align='center' class='style8'><span class='style2'>ITEM</span></div></td>"
                sBody += "		<td width='60'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>PRD RM</span></div></td>"
                sBody += "		<td width='60'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>PRD FOR</span></div></td>"
                sBody += "		<td width='60'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>SKU</span></div></td>"
                sBody += "		<td width='360'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>DESC PRODUTO</span></div></td>"
                sBody += "		<td width='50'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>QUANT</span></div></td>"
                sBody += "		<td width='60'>"
                sBody += "		<div align='center' class='style8'><span class='style2'>VALOR</span></div></td>"
                sBody += "		<td width='360'>"
                sBody += "			<div align='center' class='style8'><span class='style2'>CRITICA</span></div></td>"
                sBody += "	</tr>"
                sBody += sTable
                sBody += "</table>"
                sBody += "<br>"
                sBody += "<div align='justify' class='style10'>Esta mensagem é confidencial, destinada exclusivamente ao(s) seu(s) destinatário(s), estando protegida pelo sigilo previsto na legislação. A sua divulgação, distribuição ou reprodução indevida sujeita o infrator às sanções criminais e cíveis. Caso tenha recebido esta mensagem indevidamente, favor devolvê-la imediatamente ao remetente."
                sBody += "This message is confidential, exclusively destinated to the addressee(s), being it´s secrecy protected by law. Divulgation, distribution or unnappropriated reproduction subject the transgressor to criminal and civil sanctions. If you have received this message in error, please return it immediately to the sender."
                sBody += "</div>"
                sBody += "</body>"
                sBody += "</html>"
                mail.To.Add(sParaCadastro)
                mail.Subject = sAssunto & sTipoEmail
                mail.Body = sBody
                smtpServer.Send(mail)
                mail.Dispose()
            End If
        End If
    End Sub

    Function fValidaPedidos(ByVal caminho As String, arq As System.IO.FileInfo, ByVal xmlDoc As XmlDocument, ByVal e As System.EventArgs)
        'Cria uma instância de um documento XML
        Dim ns As New XmlNamespaceManager(xmlDoc.NameTable)
        ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe")
        Dim xpathNav As XPathNavigator = xmlDoc.CreateNavigator()
        Dim node As XPathNavigator
        Dim bVal As Boolean = True
        Dim bSai As Boolean = False
        Dim bPo As Boolean = False
        Dim x As Integer = 0
        Dim y As Integer = 0
        Dim sPo As String = ""
        Dim sCodPrd As String = ""
        Dim sStatus As String = ""
        Dim dPreco, dQuant As Double
        Dim bValItem As Double = False
        Dim sMovimento As String = ""
        Dim sSeriePo As String = ""
        Dim sSku As String = ""
        Dim bValFor As Boolean = True
        If sFilialEmitDest = sFilialSCRegra Or sFilialEmitDest = sFilialSC1Regra Then
            Using con As SqlConnection = GetConnectionXML()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT MENSAGEM FROM REGRASXML WHERE TIPO_VALIDACAO = 'PO' AND PROCESSO = 'XML' AND ORDEM = 4"
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        sMovimento = dr.Item(0).ToString
                    End If
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con.Dispose()
            End Using
            Using con As SqlConnection = GetConnectionXML()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT MENSAGEM FROM PARAMETROSLOG WHERE TIPO_VALIDACAO = 'PO' AND PROCESSO = 'XML' AND ORDEM = 5"
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        sSeriePo = dr.Item(0).ToString
                    End If
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con.Dispose()
            End Using
        Else
            Using con As SqlConnection = GetConnectionXML()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT MENSAGEM FROM PARAMETROSLOG WHERE TIPO_VALIDACAO = 'PO' AND PROCESSO = 'XML' AND ORDEM = 1"
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        sMovimento = dr.Item(0).ToString
                    End If
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con.Dispose()
            End Using
            Using con As SqlConnection = GetConnectionXML()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT MENSAGEM FROM PARAMETROSLOG WHERE TIPO_VALIDACAO = 'PO' AND PROCESSO = 'XML' AND ORDEM = 2"
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        sSeriePo = dr.Item(0).ToString
                    End If
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    oReader.Close()
                    con.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con.Dispose()
            End Using
        End If

        Dim sNumeroMov As String
        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:nNF", ns)
        sNumeroMov = Strings.Right(Space(10).Replace(" ", "0") & node.InnerXml.ToString, 10) 'Numero do Movimento 
        Dim sSerieXml As String
        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:serie", ns)
        sSerieXml = node.InnerXml.ToString 'Numero de Serie

        Dim sCnpj As String
        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:dest/nfe:CNPJ", ns)
        If node Is Nothing Then
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:dest/nfe:CPF", ns)
            sCnpj = node.InnerXml.ToString 'Cnpj do Destinatario da NF
            sCnpj = fConverteCpf(sCnpj)
        Else
            sCnpj = node.InnerXml.ToString 'Cnpj do Destinatario da NF
            If Not sCnpj = "" Then
                sCnpj = fConverteCnpj(sCnpj)
            End If
        End If
        Using con As SqlConnection = GetConnectionERP()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT ATIVO FROM FCFO WHERE CGCCFO = '" & sCnpj & "'"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                dr.Read()
                If dr.HasRows Then
                    bValFor = True
                Else
                    bValFor = False
                End If
                con.Dispose()
            Catch ex As Exception
                bValFor = False
            End Try
        End Using
        If Not bValFor Then
            Using con2 As SqlConnection = GetConnectionXML()
                Try
                    con2.Open()
                    Dim cmd2 As New SqlCommand
                    cmd2.Connection = con2
                    cmd2.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, NUMEROMOV, SERIE, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', " +
                        "'" & sCnpjEmit & "', '" & sNomeEmit & "', 'PO', '" & sNumeroMov & "', '" & sSerieXml & "', 'FORNECEDOR NAO CADASTRADO NO SISTEMA', 'C')"
                    cmd2.ExecuteReader()
                Catch ex As Exception
                    Dim cmd2 As New SqlCommand
                    cmd2.Connection = con2
                    cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd2.ExecuteReader()
                    oReader.Close()
                    con2.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con2.Dispose()
            End Using
        Else
            Using con As SqlConnection = GetConnectionERP()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "SELECT ATIVO FROM FCFO WHERE CGCCFO = '" & sCnpj & "'"
                    Dim dr As SqlDataReader = cmd.ExecuteReader()
                    dr.Read()
                    If dr.HasRows Then
                        If dr.Item(0) = 1 Then
                            bValFor = True
                        Else
                            bValFor = False
                        End If
                    End If
                    con.Dispose()
                Catch ex As Exception
                    bValFor = False
                End Try
            End Using
        End If
        If Not bValFor Then
            Using con2 As SqlConnection = GetConnectionXML()
                Try
                    con2.Open()
                    Dim cmd2 As New SqlCommand
                    cmd2.Connection = con2
                    cmd2.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, NUMEROMOV, SERIE, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', " +
                        "'" & sCnpjEmit & "', '" & sNomeEmit & "', 'PO', '" & sNumeroMov & "', '" & sSerieXml & "', 'FORNECEDOR INATIVO NO SISTEMA', 'C')"
                    cmd2.ExecuteReader()
                Catch ex As Exception
                    Dim cmd2 As New SqlCommand
                    cmd2.Connection = con2
                    cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd2.ExecuteReader()
                    oReader.Close()
                    con2.Dispose()
                    OnElapsedTime(Me, e)
                End Try
                con2.Dispose()
            End Using
        End If
        Dim bValAssociado As Boolean = False
        Using con As SqlConnection = GetConnectionXML()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT DISTINCT(FLAG_STATUS) FROM CRITICAXML WHERE PEDIDO IS NOT NULL AND FLAG_STATUS = 'A' AND NOME_XML = '" & arq.Name & "'"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                dr.Read()
                If dr.HasRows Then
                    bValAssociado = True
                End If
                con.Dispose()
            Catch ex As Exception
                bValFor = False
            End Try
        End Using
        While Not bSai
            x = x + 1
            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & x & "]/nfe:prod/nfe:xProd", ns)
            If node Is Nothing Then
                bSai = True
                x = x - 1
            Else
                sSku = ""
                If bValAssociado Then
                    Using con As SqlConnection = GetConnectionXML()
                        Try
                            con.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            Dim dr As SqlDataReader
                            cmd.CommandText = "SELECT DISTINCT(PEDIDO) FROM CRITICAXML WHERE PEDIDO IS NOT NULL AND NOME_XML = '" & arq.Name & "'"
                            dr = cmd.ExecuteReader()
                            dr.Read()
                            If dr.HasRows Then
                                sPo = Strings.Right(Space(10).Replace(" ", "0") & dr.Item(0).ToString, 10)
                            End If
                        Catch ex As Exception
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            con.Dispose()
                            OnElapsedTime(Me, e)
                        End Try
                        con.Dispose()
                    End Using
                Else
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & x & "]/nfe:prod/nfe:xPed", ns)
                    If node Is Nothing Then
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:compra/nfe:xPed", ns)
                        If node Is Nothing Then
                            Using con As SqlConnection = GetConnectionXML()
                                Try
                                    con.Open()
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    Dim dr As SqlDataReader
                                    cmd.CommandText = "SELECT DISTINCT(PEDIDO) FROM CRITICAXML WHERE PEDIDO IS NOT NULL AND NOME_XML = '" & arq.Name & "'"
                                    dr = cmd.ExecuteReader()
                                    dr.Read()
                                    If Not dr.HasRows Then
                                        bPo = True
                                    Else
                                        If Not IsDBNull(dr.Item(0)) Then
                                            If Not dr.Item(0) = "" Then
                                                sPo = Strings.Right(Space(10).Replace(" ", "0") & dr.Item(0).ToString, 10)
                                            Else
                                                bPo = True
                                            End If
                                        Else
                                            bPo = True
                                        End If
                                    End If
                                Catch ex As Exception
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                    cmd.ExecuteReader()
                                    oReader.Close()
                                    con.Dispose()
                                    OnElapsedTime(Me, e)
                                End Try
                                con.Dispose()
                            End Using
                        Else
                            sPo = Strings.Right(Space(10).Replace(" ", "0") & node.InnerXml.ToString, 10)
                        End If
                    Else
                        sPo = Strings.Right(Space(10).Replace(" ", "0") & node.InnerXml.ToString, 10)
                    End If
                End If
                If Not bPo Then
                    Using con As SqlConnection = GetConnectionERP()
                        Try
                            con.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            Dim dr As SqlDataReader
                            cmd.CommandText = "SELECT STATUS FROM TMOV WHERE NUMEROMOV = '" & sPo & "' AND CODTMV = '" & sMovimento & "' AND SERIE = '" & sSeriePo & "' AND CODFILIAL = '" & sFilialEmitDest & "'"
                            dr = cmd.ExecuteReader()
                            dr.Read()
                            If dr.HasRows Then
                                sStatus = dr.Item(0).ToString
                            End If
                        Catch ex As Exception
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            con.Dispose()
                            OnElapsedTime(Me, e)
                        End Try
                        con.Dispose()
                    End Using
                    If sStatus = "F" Then
                        bVal = False
                        Dim sPedidoRecebido As String = ""
                        Using con As SqlConnection = GetConnectionXML()
                            Try
                                con.Open()
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                Dim dr As SqlDataReader
                                cmd.CommandText = "SELECT PEDIDO FROM CRITICAXML WHERE NOME_XML = '" & arq.Name & "' AND CRITICA = 'PEDIDO JA RECEBIDO'"
                                dr = cmd.ExecuteReader()
                                dr.Read()
                                If dr.HasRows Then
                                    sPedidoRecebido = dr.Item(0).ToString
                                End If
                            Catch ex As Exception
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                cmd.ExecuteReader()
                                oReader.Close()
                                con.Dispose()
                                OnElapsedTime(Me, e)
                            End Try
                            con.Dispose()
                        End Using
                        If sPedidoRecebido = "" Or sPedidoRecebido <> sPo Then
                            Using con As SqlConnection = GetConnectionXML()
                                Try
                                    con.Open()
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, CRITICA, PEDIDO, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', " +
                                        "'" & sCnpjEmit & "', '" & sNomeEmit & "', 'PO', 'PEDIDO N.: " & sPo & " JA RECEBIDO', '" & sPo & "', 'C')"
                                    cmd.ExecuteReader()
                                Catch ex As Exception
                                    Dim cmd As New SqlCommand
                                    cmd.Connection = con
                                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                    cmd.ExecuteReader()
                                    oReader.Close()
                                    con.Dispose()
                                    OnElapsedTime(Me, e)
                                End Try
                                con.Dispose()
                            End Using
                        End If
                        Continue While
                    Else
                        If Not bPo And bVal Then
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & x & "]/nfe:prod/nfe:cProd", ns)
                            If Not node Is Nothing Then
                                sCodPrd = Strings.Left(node.InnerXml.ToString, 20)
                            Else
                                sCodPrd = ""
                            End If
                            For i = 1 To y + 1
                                If mPoString(0, (i - 1)) = sCodPrd And mPoString(1, i - 1) = sPo Then
                                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & x & "]/nfe:prod/nfe:vUnCom", ns)
                                    If Not node Is Nothing Then
                                        dPreco = node.InnerXml.ToString.Replace(".", ",")
                                    Else
                                        dPreco = 0
                                    End If
                                    If mPoDouble(0, (i - 1)) <> dPreco And sTpNf = "ENTRADA" Then
                                        bVal = False
                                        bValItem = True
                                        Using con As SqlConnection = GetConnectionXML()
                                            Try
                                                con.Open()
                                                Dim cmd As New SqlCommand
                                                cmd.Connection = con
                                                cmd.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, VALOR_PRD, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', " +
                                                    "'" & sCnpjEmit & "', '" & sNomeEmit & "', 'PO', '" & x & "', '" & sCodPrd & "', " & Str(dPreco) & ", 'ITENS NO XML COM O MESMO CODIGO DE PRODUTO COM PRECO UNITARIO DIFERENTE', 'C')"
                                                cmd.ExecuteReader()
                                            Catch ex As Exception
                                                Dim cmd As New SqlCommand
                                                cmd.Connection = con
                                                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                cmd.ExecuteReader()
                                                oReader.Close()
                                                con.Dispose()
                                                OnElapsedTime(Me, e)
                                            End Try
                                            con.Dispose()
                                        End Using
                                    Else
                                        Dim dQuantItem As Double
                                        dQuantItem = mPoDouble(1, (i - 1))
                                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & x & "]/nfe:prod/nfe:qCom", ns)
                                        If Not node Is Nothing Then
                                            dQuant = node.InnerXml.ToString.Replace(".", ",")
                                        Else
                                            dQuant = 0
                                        End If
                                        mPoDouble(1, (i - 1)) = dQuantItem + dQuant
                                        bValItem = False
                                    End If
                                End If
                            Next

                            ' If bValItem Then
                            'bValItem = False
                            'Continue While
                            'End If


                            mPoString(0, y) = sCodPrd
                            If bValAssociado Then
                                Using con As SqlConnection = GetConnectionXML()
                                    Try
                                        con.Open()
                                        Dim cmd As New SqlCommand
                                        cmd.Connection = con
                                        Dim dr As SqlDataReader
                                        cmd.CommandText = "SELECT DISTINCT(PEDIDO) FROM CRITICAXML WHERE PEDIDO IS NOT NULL AND NOME_XML = '" & arq.Name & "'"
                                        dr = cmd.ExecuteReader()
                                        dr.Read()
                                        If dr.HasRows Then
                                            sPo = Strings.Right(Space(10).Replace(" ", "0") & dr.Item(0).ToString, 10)
                                        End If
                                    Catch ex As Exception
                                        Dim cmd As New SqlCommand
                                        cmd.Connection = con
                                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                        cmd.ExecuteReader()
                                        oReader.Close()
                                        con.Dispose()
                                        OnElapsedTime(Me, e)
                                    End Try
                                    con.Dispose()
                                End Using
                            Else
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & x & "]/nfe:prod/nfe:xPed", ns)
                                If Not node Is Nothing Then
                                    sPo = Strings.Right(Space(10).Replace(" ", "0") & node.InnerXml.ToString, 10)
                                Else
                                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:compra/nfe:xPed", ns)
                                    If Not node Is Nothing Then
                                        sPo = Strings.Right(Space(10).Replace(" ", "0") & node.InnerXml.ToString, 10)
                                    Else
                                        Using con As SqlConnection = GetConnectionXML()
                                            Try
                                                con.Open()
                                                Dim cmd As New SqlCommand
                                                cmd.Connection = con
                                                Dim dr As SqlDataReader
                                                cmd.CommandText = "SELECT DISTINCT(PEDIDO) FROM CRITICAXML WHERE PEDIDO IS NOT NULL AND NOME_XML = '" & arq.Name & "'"
                                                dr = cmd.ExecuteReader()
                                                dr.Read()
                                                If dr.HasRows Then
                                                    sPo = Strings.Right(Space(10).Replace(" ", "0") & dr.Item(0).ToString, 10)
                                                Else
                                                    sPo = Space(10).Replace(" ", "0")
                                                End If
                                            Catch ex As Exception
                                                Dim cmd As New SqlCommand
                                                cmd.Connection = con
                                                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                cmd.ExecuteReader()
                                                oReader.Close()
                                                con.Dispose()
                                                OnElapsedTime(Me, e)
                                            End Try
                                            con.Dispose()
                                        End Using
                                    End If
                                End If
                            End If
                            mPoString(1, y) = sPo
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & x & "]/nfe:prod/nfe:vUnCom", ns)
                            If Not node Is Nothing Then
                                dPreco = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dPreco = 0
                            End If
                            mPoDouble(0, y) = dPreco
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & x & "]/nfe:prod/nfe:qCom", ns)
                            If Not node Is Nothing Then
                                dQuant = node.InnerXml.ToString.Replace(".", ",")
                            Else
                                dQuant = 0
                            End If
                            mPoDouble(1, y) = dQuant
                            y = y + 1
                            ReDim Preserve mPoString(4, y)
                            ReDim Preserve mPoDouble(1, y)
                        End If
                    End If
                Else
                    bVal = False
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & x & "]/nfe:prod/nfe:cProd", ns)
                    If Not node Is Nothing Then
                        sCodPrd = Strings.Left(node.InnerXml.ToString, 20)
                    Else
                        sCodPrd = ""
                    End If
                    Dim sDescPrdFor As String
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & x & "]/nfe:prod/nfe:xProd", ns)
                    If Not node Is Nothing Then
                        sDescPrdFor = node.InnerXml.ToString
                    Else
                        sDescPrdFor = ""
                    End If
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & x & "]/nfe:prod/nfe:qCom", ns)
                    If Not node Is Nothing Then
                        dQuant = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dQuant = 0
                    End If
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & x & "]/nfe:prod/nfe:vUnCom", ns)
                    If Not node Is Nothing Then
                        dPreco = node.InnerXml.ToString.Replace(".", ",")
                    Else
                        dPreco = 0
                    End If
                    Dim sIdPrd As String = ""
                    Using con As SqlConnection = GetConnectionERP() 'Verifica o cadastro do produto
                        Try
                            con.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "SELECT IDPRD FROM TPRDCFO WHERE CODNOFORN = '" & sCodPrd & "' AND CODCFO = '" & sCodEmpEmitDest & "'"
                            Dim dr As SqlDataReader = cmd.ExecuteReader()
                            dr.Read()
                            If dr.HasRows Then
                                sIdPrd = dr.Item(0).ToString
                            Else
                                sIdPrd = "0"
                            End If
                        Catch ex As Exception
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            con.Dispose()
                            OnElapsedTime(Me, e)
                        End Try
                        con.Dispose()
                    End Using
                    Using con As SqlConnection = GetConnectionERP()
                        Try
                            con.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "SELECT CODIGOAUXILIAR FROM TPRODUTO WHERE IDPRD = " & sIdPrd & ""
                            Dim dr As SqlDataReader = cmd.ExecuteReader()
                            dr.Read()
                            If dr.HasRows Then
                                sSku = dr.Item(0).ToString
                            Else
                                sSku = ""
                            End If
                        Catch ex As Exception
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            con.Dispose()
                            OnElapsedTime(Me, e)
                        End Try
                        con.Dispose()
                    End Using
                    Using con As SqlConnection = GetConnectionXML()
                        Try
                            con.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "INSERT INTO CRITICAXML (NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, CRITICA, CODFOR, NUMEROMOV, SERIE, ITEM_XML, COD_PRD, COD_PRD_AUX, SKU, DESC_PRD, QUANTIDADE, VALOR_PRD, CODFILIAL, TIPO) VALUES (" +
                                "'" & arq.Name & "', '" & sDataEmissao & "', '" & sCnpjEmit & "', '" & sNomeEmit & "', 'PO', 'PEDIDO DE COMPRA NAO DESTACADO NO XML', '" & sCodEmpEmitDest & "', '" & sNumeroMov & "', '" & sSerieXml & "', " +
                                "'" & x & "', '" & sIdPrd & "', '" & sCodPrd & "', '" & sSku & "', '" & sDescPrdFor & "', '" & Str(dQuant).Replace(" ", "") & "', '" & Str(dPreco).Replace(" ", "") & "', '" & sFilialEmitDest & "', " +
                            "'C')"
                            cmd.ExecuteReader()
                        Catch ex As Exception
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            con.Dispose()
                            OnElapsedTime(Me, e)
                        End Try
                        con.Dispose()
                    End Using
                End If
            End If
        End While
        iQuantPo = y

        If bVal Then
            Dim iLinhas As Integer = y
            If y < 1 Then
                iLinhas = y - 1
            End If
            'Dim bPegaMov As Boolean = True
            For i = 1 To iLinhas
                sSku = ""
                Dim iIdMovPo As Integer = 0
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        Dim dr As SqlDataReader
                        cmd.CommandText = "SELECT IDMOV FROM TMOV WHERE CODTMV = '" & sMovimento & "' AND NUMEROMOV = '" & mPoString(1, (i - 1)) & "' AND SERIE = '" & sSeriePo & "' AND CODCFO = '" & sCodEmpEmitDest & "'"
                        dr = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            If IsDBNull(dr.Item(0)) Then
                                bVal = False
                                Using con2 As SqlConnection = GetConnectionXML()
                                    Try
                                        con2.Open()
                                        Dim cmd2 As New SqlCommand
                                        cmd2.Connection = con2
                                        cmd2.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, PEDIDO, NUMEROMOV, SERIE, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', " +
                                            "'" & sCnpjEmit & "', '" & sNomeEmit & "', 'PO', '" & mPoString(1, (i - 1)) & "', '" & sNumeroMov & "', '" & sSerieXml & "', 'P.O. " & mPoString(1, (i - 1)) & " INEXISTENTE PARA ESTE FORNECEDOR', 'C')"
                                        cmd2.ExecuteReader()
                                    Catch ex2 As Exception
                                        Dim cmd2 As New SqlCommand
                                        cmd2.Connection = con2
                                        cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex2.Message & "')"
                                        cmd2.ExecuteReader()
                                        oReader.Close()
                                        con2.Dispose()
                                        OnElapsedTime(Me, e)
                                    End Try
                                    con2.Dispose()
                                End Using
                            Else
                                If sFilialEmitDest = sFilialSC1Regra Or sFilialEmitDest = sFilialSCRegra Then
                                    Using con2 As SqlConnection = GetConnectionERP()
                                        Try
                                            con2.Open()
                                            Dim cmd2 As New SqlCommand
                                            cmd2.Connection = con2
                                            cmd2.CommandText = "SELECT IDMOV FROM TMOVAPROVA WHERE IDMOV = '" & dr.Item(0) & "'"
                                            Dim dr2 As SqlDataReader = cmd2.ExecuteReader()
                                            dr2.Read()
                                            If dr2.HasRows Then
                                                iIdMovPo = dr2.Item(0)
                                            Else
                                                bVal = False
                                                Using con3 As SqlConnection = GetConnectionXML()
                                                    Try
                                                        con3.Open()
                                                        Dim cmd3 As New SqlCommand
                                                        cmd3.Connection = con3
                                                        cmd3.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, PEDIDO, NUMEROMOV, SERIE, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', " +
                                                            "'" & sCnpjEmit & "', '" & sNomeEmit & "', 'PO', '" & mPoString(1, (i - 1)) & "', '" & sNumeroMov & "', '" & sSerieXml & "', 'P.O. " & mPoString(1, (i - 1)) & " SEM APROVACAO', 'C')"
                                                        cmd3.ExecuteReader()
                                                    Catch ex3 As Exception
                                                        Dim cmd3 As New SqlCommand
                                                        cmd3.Connection = con3
                                                        cmd3.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex3.Message & "')"
                                                        cmd3.ExecuteReader()
                                                        oReader.Close()
                                                        con3.Dispose()
                                                        OnElapsedTime(Me, e)
                                                    End Try
                                                    con3.Dispose()
                                                End Using
                                            End If
                                        Catch ex2 As Exception
                                            Dim cmd2 As New SqlCommand
                                            cmd2.Connection = con2
                                            cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex2.Message & "')"
                                            cmd2.ExecuteReader()
                                            oReader.Close()
                                            con2.Dispose()
                                            OnElapsedTime(Me, e)
                                        End Try
                                        con2.Dispose()
                                    End Using
                                Else
                                    iIdMovPo = dr.Item(0)
                                End If
                            End If
                        Else
                            bVal = False
                            Using con2 As SqlConnection = GetConnectionXML()
                                Try
                                    con2.Open()
                                    Dim cmd2 As New SqlCommand
                                    cmd2.Connection = con2
                                    cmd2.CommandText = "INSERT INTO CRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, PEDIDO, NUMEROMOV, SERIE, CRITICA, TIPO) VALUES ('" & sFilialEmitDest & "', '" & arq.Name & "', '" & sDataEmissao & "', " +
                                        "'" & sCnpjEmit & "', '" & sNomeEmit & "', 'PO', '" & mPoString(1, (i - 1)) & "', '" & sNumeroMov & "', '" & sSerieXml & "', 'P.O. " & mPoString(1, (i - 1)) & " INEXISTENTE PARA ESTE FORNECEDOR', 'C')"
                                    cmd2.ExecuteReader()
                                Catch ex As Exception
                                    Dim cmd2 As New SqlCommand
                                    cmd2.Connection = con2
                                    cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                    cmd2.ExecuteReader()
                                    oReader.Close()
                                    con2.Dispose()
                                    OnElapsedTime(Me, e)
                                End Try
                                con2.Dispose()
                            End Using
                        End If
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using
                Dim sNomeProd As String = ""
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:xProd", ns)
                If Not node Is Nothing Then
                    sNomeProd = node.InnerXml.ToString
                Else
                    sNomeProd = ""
                End If
                Dim sCodProd As String = ""
                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" & i & "]/nfe:prod/nfe:cProd", ns)
                If Not node Is Nothing Then
                    sCodProd = Strings.Left(node.InnerXml.ToString, 20)
                Else
                    sCodProd = ""
                End If
                Dim bProdCad As Boolean = True
                Dim iIdPrd As Integer = 0
                If sTpNf = "ENTRADA" Then
                    Using con2 As SqlConnection = GetConnectionERP() 'Verifica o cadastro do produto
                        Try
                            con2.Open()
                            Dim cmd2 As New SqlCommand
                            cmd2.Connection = con2
                            cmd2.CommandText = "SELECT IDPRD FROM TPRDCFO WHERE CODNOFORN = '" & sCodProd & "' AND CODCFO = '" & sCodEmpEmitDest & "'"
                            Dim dr2 As SqlDataReader = cmd2.ExecuteReader()
                            dr2.Read()
                            If dr2.HasRows Then
                                iIdPrd = dr2.Item(0)
                            Else
                                bProdCad = False
                            End If
                        Catch ex As Exception
                            Dim cmd2 As New SqlCommand
                            cmd2.Connection = con2
                            cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd2.ExecuteReader()
                            oReader.Close()
                            con2.Dispose()
                            OnElapsedTime(Me, e)
                        End Try
                        con2.Dispose()
                    End Using
                ElseIf sTpNf = "ENT_IMP" Then
                    Using con2 As SqlConnection = GetConnectionERP() 'Verifica o cadastro do produto
                        Try
                            con2.Open()
                            Dim cmd2 As New SqlCommand
                            cmd2.Connection = con2
                            cmd2.CommandText = "SELECT IDPRD FROM TPRODUTO WHERE CODIGOAUXILIAR = '" & sCodProd & "'"
                            Dim dr2 As SqlDataReader = cmd2.ExecuteReader()
                            dr2.Read()
                            If dr2.HasRows Then
                                iIdPrd = dr2.Item(0)
                            Else
                                bProdCad = False
                            End If
                        Catch ex As Exception
                            Dim cmd2 As New SqlCommand
                            cmd2.Connection = con2
                            cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd2.ExecuteReader()
                            oReader.Close()
                            con2.Dispose()
                            OnElapsedTime(Me, e)
                        End Try
                        con2.Dispose()
                    End Using
                End If
                Using con2 As SqlConnection = GetConnectionERP() 'Verifica o SKU do produto
                    Try
                        con2.Open()
                        Dim cmd2 As New SqlCommand
                        cmd2.Connection = con2
                        cmd2.CommandText = "SELECT CODIGOAUXILIAR FROM TPRODUTO WHERE IDPRD = " & iIdPrd & ""
                        Dim dr2 As SqlDataReader = cmd2.ExecuteReader()
                        dr2.Read()
                        If dr2.HasRows Then
                            sSku = dr2.Item(0).ToString
                        Else
                            bProdCad = False
                        End If
                    Catch ex As Exception
                        Dim cmd2 As New SqlCommand
                        cmd2.Connection = con2
                        cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd2.ExecuteReader()
                        oReader.Close()
                        con2.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con2.Dispose()
                End Using
                If Not bProdCad Then
                    bVal = False
                    Using con2 As SqlConnection = GetConnectionXML()
                        Try
                            con2.Open()
                            Dim cmd2 As New SqlCommand
                            cmd2.Connection = con2
                            cmd2.CommandText = "INSERT INTO CRITICAXML (NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, SKU, CODFILIAL, DESC_PRD, PEDIDO, NUMEROMOV, SERIE, CRITICA, TIPO) VALUES ('" & arq.Name & "', '" & sDataEmissao & "', " +
                                "'" & sCnpjEmit & "', '" & sNomeEmit & "', 'PO', '" & i & "', '" & sCodProd & "', '" & sSku & "', '" & sFilialEmitDest & "', '" & sNomeProd & "' , '" & mPoString(1, (i - 1)) & "', '" & sNumeroMov & "', '" & sSerieXml & "', 'PRODUTO " & sNomeProd & " NAO CADASTRADO NO SISTEMA', 'C')"
                            cmd2.ExecuteReader()
                        Catch ex As Exception
                            Dim cmd2 As New SqlCommand
                            cmd2.Connection = con2
                            cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd2.ExecuteReader()
                            oReader.Close()
                            con2.Dispose()
                            OnElapsedTime(Me, e)
                        End Try
                        con2.Dispose()
                    End Using
                End If

                Dim bValProdPo As Boolean = True
                Using con As SqlConnection = GetConnectionERP()
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        Dim dr As SqlDataReader
                        cmd.CommandText = "SELECT QUANTIDADE, PRECOUNITARIO FROM TITMMOV WHERE IDMOV = " & iIdMovPo & " AND IDPRD = " & iIdPrd & ""
                        dr = cmd.ExecuteReader()
                        dr.Read()
                        If dr.HasRows Then
                            If IsDBNull(dr.Item(0)) Then
                                bVal = False
                                bValProdPo = False
                            Else
                                mPoString(2, (i - 1)) = iIdPrd.ToString
                                mPoString(3, (i - 1)) = sMovimento
                                mPoString(4, (i - 1)) = iIdMovPo
                                If dr.Item(0) >= Round(mPoDouble(1, (i - 1)), 4) Then
                                    If dr.Item(1) = Round(mPoDouble(0, (i - 1)), 4) Then
                                        'XML X PO corretos
                                        bValPoOk = True
                                    Else
                                        bVal = False
                                        Using con2 As SqlConnection = GetConnectionXML()
                                            Try
                                                con2.Open()
                                                Dim cmd2 As New SqlCommand
                                                cmd2.Connection = con2
                                                cmd2.CommandText = "INSERT INTO CRITICAXML (NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, SKU, CODFILIAL, VALOR_PRD, DESC_PRD, CRITICA, TIPO) VALUES ('" & arq.Name & "', '" & sDataEmissao & "', " +
                                                    "'" & sCnpjEmit & "', '" & sNomeEmit & "', 'PO', '" & i & "', '" & iIdPrd.ToString & "', '" & sCodProd & "', '" & sSku & "', '" & sFilialEmitDest & "', '" & Str(dr.Item(1)) & "', '" & sNomeProd & "', 'PRECO DO PRODUTO " & sNomeProd & " DIFERENTE. P.O. N.: " & mPoString(1, (i - 1)) & " - PRECO P.O. " & Str(dr.Item(1)) & " ', 'C')"
                                                cmd2.ExecuteReader()
                                            Catch ex As Exception
                                                Dim cmd2 As New SqlCommand
                                                cmd2.Connection = con2
                                                cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                                cmd2.ExecuteReader()
                                                oReader.Close()
                                                con2.Dispose()
                                                OnElapsedTime(Me, e)
                                            End Try
                                            con2.Dispose()
                                        End Using
                                    End If
                                Else
                                    bVal = False
                                    Using con2 As SqlConnection = GetConnectionXML()
                                        Try
                                            con2.Open()
                                            Dim cmd2 As New SqlCommand
                                            cmd2.Connection = con2
                                            cmd2.CommandText = "INSERT INTO CRITICAXML (NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD, COD_PRD_AUX, SKU, CODFILIAL, DESC_PRD, QUANTIDADE, VALOR_PRD, CRITICA, TIPO) VALUES ('" & arq.Name & "', '" & sDataEmissao & "', " +
                                                "'" & sCnpjEmit & "', '" & sNomeEmit & "', 'PO', '" & i & "', '" & iIdPrd.ToString & "', '" & sCodProd & "', '" & sSku & "', '" & sFilialEmitDest & "', '" & sNomeProd & "', '" & Str(mPoDouble(1, (i - 1))) & "' , '" & Str(mPoDouble(0, (i - 1))) & "', 'QUANTIDADE DO PRODUTO " & sNomeProd & " MAIOR DO QUE SOLICITADO. P.O. N.: " & mPoString(1, (i - 1)) & " - QUANTIDADE SOLICITADA " & Math.Round(dr.Item(0)).ToString & " -- QUANTIDADE ENVIADA " & Math.Round(mPoDouble(1, (i - 1))).ToString & "', 'C')"
                                            cmd2.ExecuteReader()
                                        Catch ex As Exception
                                            Dim cmd2 As New SqlCommand
                                            cmd2.Connection = con2
                                            cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                            cmd2.ExecuteReader()
                                            oReader.Close()
                                            con2.Dispose()
                                            OnElapsedTime(Me, e)
                                        End Try
                                        con2.Dispose()
                                    End Using
                                End If
                            End If
                        Else
                            bValProdPo = False
                        End If
                        If Not bValProdPo Then
                            bVal = False
                            Using con2 As SqlConnection = GetConnectionXML()
                                Try
                                    con2.Open()
                                    Dim cmd2 As New SqlCommand
                                    cmd2.Connection = con2
                                    cmd2.CommandText = "INSERT INTO CRITICAXML (NOME_XML, DATAEMISSAO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, SKU, CODFILIAL, DESC_PRD, PEDIDO, NUMEROMOV, SERIE, QUANTIDADE, VALOR_PRD, CRITICA, TIPO) VALUES ('" & arq.Name & "', '" & sDataEmissao & "', " +
                                        "'" & sCnpjEmit & "', '" & sNomeEmit & "', 'PO', '" & i & "', '" & sCodProd & "', '" & sSku & "', '" & sFilialEmitDest & "', '" & sNomeProd & "', '" & mPoString(1, (i - 1)) & "', '" & sNumeroMov & "', '" & sSerieXml & "', '" & Str(mPoDouble(1, (i - 1))) & "' , '" & Str(mPoDouble(0, (i - 1))) & "', 'PRODUTO " & sNomeProd & " INEXISTENTE NA P.O. N.: " & mPoString(1, (i - 1)) & "', 'C')"
                                    cmd2.ExecuteReader()
                                Catch ex As Exception
                                    Dim cmd2 As New SqlCommand
                                    cmd2.Connection = con2
                                    cmd2.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                    cmd2.ExecuteReader()
                                    oReader.Close()
                                    con2.Dispose()
                                    OnElapsedTime(Me, e)
                                End Try
                                con2.Dispose()
                            End Using
                        End If
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using
            Next
        End If
        Return bVal
    End Function

    Private Function VerificarPO(ByVal e As System.EventArgs) As Integer
        Using con As SqlConnection = GetConnectionXML()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT NOME_XML FROM CRITICAXML WHERE NOME_XML='" & arq.Name & "' AND SETOR='PO' AND PEDIDO IS NOT NULL"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                dr.Read()
                If dr.HasRows Then
                    iQuantPo = 1
                Else
                    iQuantPo = 0
                End If
            Catch ex As Exception
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                oReader.Close()
                con.Dispose()
                OnElapsedTime(Me, e)
            Finally
                con.Dispose()
            End Try
        End Using

        Return iQuantPo
    End Function

    Sub CarregaApp(ByVal e As System.EventArgs)
        sEnvioHoraDiario1 = configurationAppSettings.GetValue("HoraEnvioDiario1", GetType(System.String))
        sEnvioHoraDiario2 = configurationAppSettings.GetValue("HoraEnvioDiario2", GetType(System.String))
        sEnvioHoraDiario3 = configurationAppSettings.GetValue("HoraEnvioDiario3", GetType(System.String))
        sEnvioHoraDiario4 = configurationAppSettings.GetValue("HoraEnvioDiario4", GetType(System.String))
        caminho = configurationAppSettings.GetValue("RepositorioXML", GetType(System.String))
        sProcessado = configurationAppSettings.GetValue("ProcessadosXML", GetType(System.String))
        sManual = configurationAppSettings.GetValue("ManualXML", GetType(System.String))
        sCriticados = configurationAppSettings.GetValue("CriticadosXML", GetType(System.String))
        sLogo = configurationAppSettings.GetValue("Logo", GetType(System.String))
        sCodUsuario = configurationAppSettings.GetValue("User", GetType(System.String))
        sSigamatOrig = configurationAppSettings.GetValue("OrigemSigaMat", GetType(System.String))
        sSigamatDest = configurationAppSettings.GetValue("DestinoSigaMat", GetType(System.String))
    End Sub

    Sub fLeRegras(ByVal e As System.EventArgs)

        If bAtivaLog Then
            fLog(arq.Name, "Função fLeRegras ")
        End If

        Using con As SqlConnection = GetConnectionXML()
            Try
                con.Open()
                fLog(arq.Name, "Abriu Banco CST")
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT CONTEUDO, ORDEM FROM REGRASXML WHERE PROCESSO = 'CST' ORDER BY ORDEM"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                If dr.HasRows Then
                    While dr.Read()
                        Select Case dr.Item(1)
                            Case 1
                                sCstRegra = dr.Item(0).ToString
                            Case 2
                                sCstComReducao = dr.Item(0).ToString
                            Case 3
                                sCstComAliqIcms = dr.Item(0).ToString
                        End Select
                    End While
                Else
                    sCstRegra = ""
                    sCstComReducao = ""
                    sCstComAliqIcms = ""
                End If
            Catch ex As Exception
                fLog(arq.Name, ex.ToString())

                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "',convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                oReader.Close()
                con.Dispose()
                OnElapsedTime(Me, e)
            End Try
            con.Dispose()
        End Using
        Using con As SqlConnection = GetConnectionXML()
            Try
                fLog(arq.Name, "Abriu Banco CFOP")
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT CONTEUDO, ORDEM FROM REGRASXML WHERE PROCESSO = 'CFOP' ORDER BY ORDEM"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                If dr.HasRows Then
                    While dr.Read()
                        Select Case dr.Item(1)
                            Case 1
                                sComCfopRegra = dr.Item(0).ToString
                            Case 2
                                sCupomFiscalRegra = dr.Item(0).ToString
                            Case 3
                                sOutrosMercServRegra = dr.Item(0).ToString
                            Case 4
                                sMercRevComStRegra = dr.Item(0).ToString
                            Case 5
                                sMercRevSemStRegra = dr.Item(0).ToString
                            Case 6
                                sUsoComStRegra = dr.Item(0).ToString
                            Case 7
                                sUsoSemStRegra = dr.Item(0).ToString
                            Case 8
                                sRetConsertRegra = dr.Item(0).ToString
                            Case 9
                                sAtivoComStRegra = dr.Item(0).ToString
                            Case 10
                                sAtivoSemStRegra = dr.Item(0).ToString
                            Case 11
                                sIssRegra = dr.Item(0).ToString
                        End Select
                    End While
                End If
            Catch ex As Exception
                fLog(arq.Name, "Erro CFOP" & ex.ToString())
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                oReader.Close()
                con.Dispose()
                OnElapsedTime(Me, e)
            End Try
            con.Dispose()
        End Using
        Using con As SqlConnection = GetConnectionXML()
            Try
                fLog(arq.Name, "Abriu banco SITMERC")
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT CONTEUDO, ORDEM FROM REGRASXML WHERE PROCESSO = 'SITMERC' ORDER BY ORDEM"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                If dr.HasRows Then
                    While dr.Read()
                        Select Case dr.Item(1)
                            Case 1
                                sSitMercRevRegra = dr.Item(0).ToString
                            Case 2
                                sSitMercUsoRegra = dr.Item(0).ToString
                            Case 3
                                sSitMercAtivoRegra = dr.Item(0).ToString
                        End Select
                    End While
                End If
            Catch ex As Exception
                fLog(arq.Name, "Erro SITMERC" & ex.ToString())
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                oReader.Close()
                con.Dispose()
                OnElapsedTime(Me, e)
            End Try
            con.Dispose()
        End Using
        Using con As SqlConnection = GetConnectionXML()
            Try
                fLog(arq.Name, "Abriu banco CODFILIAL")
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT CONTEUDO, ORDEM FROM REGRASXML WHERE PROCESSO = 'CODFILIAL' ORDER BY ORDEM"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                If dr.HasRows Then
                    While dr.Read()
                        Select Case dr.Item(1)
                            Case 1
                                sFilialSCRegra = dr.Item(0).ToString
                            Case 2
                                sFilialSC1Regra = dr.Item(0).ToString
                            Case 3
                                sFilialCDRegra = dr.Item(0).ToString
                        End Select
                    End While
                End If
            Catch ex As Exception
                fLog(arq.Name, "Erro Codfilial" & ex.ToString())
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                oReader.Close()
                con.Dispose()
                OnElapsedTime(Me, e)
            End Try
            con.Dispose()
        End Using
    End Sub

    Function fVerificaXmlCriticado(ByVal arq As String, ByVal e As System.EventArgs)

        If bAtivaLog Then
            fLog(arq, "Função fVerificaXmlCriticado")
        End If

        Using con As SqlConnection = GetConnectionXML()
            con.Open()
            Dim cmd As New SqlCommand
            cmd.Connection = con
            Dim dr As SqlDataReader
            cmd.CommandText = "SELECT DISTINCT (FLAG_STATUS) FROM CRITICAXML WHERE FLAG_STATUS IS NULL AND NOME_XML = '" & arq & "'"
            dr = cmd.ExecuteReader()
            If dr.HasRows Then
                Using con2 As SqlConnection = GetConnectionXML()
                    con2.Open()
                    Dim cmd2 As New SqlCommand
                    cmd2.Connection = con2
                    Dim dr2 As SqlDataReader
                    cmd2.CommandText = "UPDATE CRITICAXML SET FLAG_STATUS = 'E' WHERE FLAG_STATUS IS NULL AND NOME_XML = '" & arq & "'"
                    dr2 = cmd2.ExecuteReader()
                    con2.Dispose()
                End Using
            End If
            con.Dispose()
        End Using
        Using con As SqlConnection = GetConnectionXML()
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                Dim dr As SqlDataReader
                cmd.CommandText = "SELECT DISTINCT (FLAG_STATUS) FROM CRITICAXML WHERE FLAG_STATUS = 'E' AND NOME_XML = '" & arq & "'"
                dr = cmd.ExecuteReader()
                If dr.HasRows Then
                    bValEnv = True
                End If
            Catch ex As Exception
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                oReader.Close()
                File.Delete(sManual & "\" & arq)
                File.Copy(caminho & "\" & arq, sManual & "\" & arq)
                File.Delete(caminho & "\" & arq)
                con.Dispose()
                OnElapsedTime(Me, e)
            End Try
            con.Dispose()
        End Using
        Return bValEnv
    End Function

    Function fValidaNotaLancada(ByVal sChaveNfe As String, ByVal e As System.EventArgs)

        If bAtivaLog Then
            fLog(arq.Name, "Função ValidaNotaLancada")
        End If

        Dim bVal As Boolean = False
        If sTpNf = "ENTRADA" Or sTpNf = "TRANSF_ENT" Then
            If fvalXmlProcessadoTransf(sChaveNfe, sCodTabEmitDest) Then
                Using con As SqlConnection = GetConnectionXML()  'Grava log
                    Try
                        con.Open()
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO LOGEVENTOSXML (NOME_XML, DATAEMISSAO, SETOR, USUARIO, EVENTO, CRITICA) " +
                            "VALUES ('" & arq.Name & "',convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), 'FIS', '" & sCodUsuario & "', 'I', 'XML TRANSFERIDO PARA A PASTA CRITICADOS, O MESMO JA FOI LANCADO NO SISTEMA.')"
                        cmd.ExecuteReader()
                    Catch ex As Exception
                        Dim cmd As New SqlCommand
                        cmd.Connection = con
                        cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                        cmd.ExecuteReader()
                        oReader.Close()
                        con.Dispose()
                        OnElapsedTime(Me, e)
                    End Try
                    con.Dispose()
                End Using
                oReader.Close()
                File.Delete(sCriticados & "\" & arq.Name)
                File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                File.Delete(caminho & "\" & arq.Name)
                bVal = True
            End If
        Else
            If fvalXmlProcessado(sChaveNfe, sCodTabEmitDest) Then
                If sTpNf = "TRANSF_ENTSAI" Then
                    sTpNf = "TRANSF_ENT"
                    If fvalXmlProcessadoTransf(sChaveNfe, sCodTabEmitDest) Then
                        Using con As SqlConnection = GetConnectionXML()  'Grava log
                            Try
                                con.Open()
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                cmd.CommandText = "INSERT INTO LOGEVENTOSXML (NOME_XML, DATAEMISSAO, SETOR, USUARIO, EVENTO, CRITICA) " +
                                    "VALUES ('" & arq.Name & "',convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), 'FIS', '" & sCodUsuario & "', 'I', 'XML TRANSFERIDO PARA A PASTA CRITICADOS, O MESMO JA FOI LANCADO NO SISTEMA.')"
                                cmd.ExecuteReader()
                            Catch ex As Exception
                                Dim cmd As New SqlCommand
                                cmd.Connection = con
                                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                                cmd.ExecuteReader()
                                oReader.Close()
                                con.Dispose()
                                OnElapsedTime(Me, e)
                            End Try
                            con.Dispose()
                        End Using
                        oReader.Close()
                        File.Delete(sCriticados & "\" & arq.Name)
                        File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                        File.Delete(caminho & "\" & arq.Name)
                        bVal = True
                    End If
                ElseIf sTpNf = "ENTRADA" Or sTpNf = "ENT_IMP" Then
                    Using con As SqlConnection = GetConnectionXML()  'Grava log
                        Try
                            con.Open()
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "INSERT INTO LOGEVENTOSXML (NOME_XML, DATAEMISSAO, SETOR, USUARIO, EVENTO, CRITICA) " +
                                "VALUES ('" & arq.Name & "',convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), 'FIS', '" & sCodUsuario & "', 'I', 'XML TRANSFERIDO PARA A PASTA CRITICADOS, O MESMO JA FOI LANCADO NO SISTEMA.')"
                            cmd.ExecuteReader()
                        Catch ex As Exception
                            Dim cmd As New SqlCommand
                            cmd.Connection = con
                            cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" & arq.Name & "', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                            cmd.ExecuteReader()
                            oReader.Close()
                            con.Dispose()
                            OnElapsedTime(Me, e)
                        End Try
                        con.Dispose()
                    End Using
                    oReader.Close()
                    File.Delete(sCriticados & "\" & arq.Name)
                    File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                    File.Delete(caminho & "\" & arq.Name)
                    bVal = True
                End If
            End If
        End If
        Return bVal
    End Function

    Sub fEnviaEmalDiario(ByVal e As System.EventArgs)
        'Envio de emails diarios
        If DateTime.Now.ToString.Substring(11, 8) > "00:00:00" And DateTime.Now.ToString.Substring(11, 8) < "00:00:10" Then 'Atualiza o flag para enviar email do dia seguinte
            Using con As SqlConnection = GetConnectionXML()
                Try
                    con.Open()
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    Dim dr As SqlDataReader
                    cmd.CommandText = "UPDATE ZPARAMETROSLOG SET ATIVO = 0 WHERE TIPO_VALIDACAO IN ('ENVIADO1', 'ENVIADO2', 'ENVIADO3', 'ENVIADO4')"
                    dr = cmd.ExecuteReader()
                Catch ex As Exception
                    Dim cmd As New SqlCommand
                    cmd.Connection = con
                    cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('ATU FLAG EMAIL DIA SEGUINTE', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                    cmd.ExecuteReader()
                    con.Dispose()
                End Try
                con.Dispose()
            End Using
        End If

        Dim iEmailDiario1, iEmailDiario2, iEmailDiario3, iEmailDiario4 As Integer

        Using con As SqlConnection = GetConnectionXML() 'Verifica se os emails diários foram enviados
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT ATIVO, TIPO_VALIDACAO FROM REGRASXML WHERE TIPO_VALIDACAO IN ('ENVIADO1', 'ENVIADO2', 'ENVIADO3', 'ENVIADO4') ORDER BY TIPO_VALIDACAO"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                While dr.Read()
                    If dr.HasRows Then
                        Select Case dr.Item(1)
                            Case "ENVIADO1"
                                iEmailDiario1 = dr.Item(0) * -1
                            Case "ENVIADO2"
                                iEmailDiario2 = dr.Item(0) * -1
                            Case "ENVIADO3"
                                iEmailDiario3 = dr.Item(0) * -1
                            Case "ENVIADO4"
                                iEmailDiario4 = dr.Item(0) * -1
                        End Select
                    End If
                End While
            Catch ex As Exception
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('ATUALIZA FLAG EMAIL DIARIO', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                con.Dispose()
            End Try
            con.Dispose()
        End Using

        Dim iDiario1, iDiario2, iDiario3, iDiario4 As Integer

        Using con As SqlConnection = GetConnectionXML() 'Verifica quais emails estão ativos para o envio
            Try
                con.Open()
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "SELECT ATIVO, TIPO_VALIDACAO FROM REGRASXML WHERE TIPO_VALIDACAO IN ('DIARIO1', 'DIARIO2', 'DIARIO3', 'DIARIO4') ORDER BY TIPO_VALIDACAO"
                Dim dr As SqlDataReader = cmd.ExecuteReader()
                While dr.Read()
                    If dr.HasRows Then
                        Select Case dr.Item(1)
                            Case "DIARIO1"
                                iDiario1 = dr.Item(0) * -1
                            Case "DIARIO2"
                                iDiario2 = dr.Item(0) * -1
                            Case "DIARIO3"
                                iDiario3 = dr.Item(0) * -1
                            Case "DIARIO4"
                                iDiario4 = dr.Item(0) * -1
                        End Select
                    End If
                End While
            Catch ex As Exception
                Dim cmd As New SqlCommand
                cmd.Connection = con
                cmd.CommandText = "INSERT INTO ERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('VERIFICA EMAIL ATIVO P/ ENVIO', convert(datetime, '" & String.Format("{0:yyyy-MM-dd}", Today) & "', 121), '" & ex.Message & "')"
                cmd.ExecuteReader()
                con.Dispose()
            End Try
            con.Dispose()
        End Using

        If iDiario1 = 1 And iEmailDiario1 = 0 Then 'Dispara os emails diários conforme horarios e emails ativos
            If DateTime.Now.ToString.Substring(11, 8) > sEnvioHoraDiario1 Then
                EnviaEmailDiario("ENVIADO1", e)
            End If
        End If

        If iDiario2 = 1 And iEmailDiario2 = 0 Then
            If DateTime.Now.ToString.Substring(11, 8) > sEnvioHoraDiario2 Then
                EnviaEmailDiario("ENVIADO2", e)
            End If
        End If
        If iDiario3 = 1 And iEmailDiario3 = 0 Then
            If DateTime.Now.ToString.Substring(11, 8) > sEnvioHoraDiario3 Then
                EnviaEmailDiario("ENVIADO3", e)
            End If
        End If
        If iDiario4 = 1 And iEmailDiario4 = 0 Then
            If DateTime.Now.ToString.Substring(11, 8) > sEnvioHoraDiario4 Then
                EnviaEmailDiario("ENVIADO4", e)
            End If
        End If
    End Sub

End Class