Imports System.Data.SqlClient

Public Class DB
    Public Property banco As String
    Public Property senha As String
    Public Property servidor As String
    Public Property usuario As String
    Public Property connectionString As String


    Public Sub New(ByVal servidor As String, ByVal banco As String, ByVal usuario As String, ByVal senha As String)
        connectionString = "Data Source = " + servidor + "; Initial Catalog = " + banco + "; User Id =" + usuario + "; Password =" + senha + ";Pooling=False;"


    End Sub
    Public Sub abreConexao()
        Try
        Catch ex As Exception
            Module1.fLog("Erro no banco de daados", ex.ToString())
        End Try

    End Sub
    Public Function consulta(ByVal query As String, Optional ByVal errorQuery As String = vbNullString) As ArrayList
        Dim result As New ArrayList()
        Dim conn = New SqlConnection(connectionString)
        Try
            conn.Open()
            Try


                Dim cmd As New SqlCommand
                cmd.Connection = conn
                Dim Dr As SqlDataReader
                cmd.CommandText = query
                Dr = cmd.ExecuteReader()
                While Dr.Read()
                    ' Insert each column into a dictionary
                    Dim dict As New Dictionary(Of String, Object)
                    For count As Integer = 0 To (Dr.FieldCount - 1)
                        dict.Add(Dr.GetName(count), Dr(count))
                    Next

                    ' Add the dictionary to the ArrayList
                    result.Add(dict)
                End While
                Dr.Close()
                conn.Close()
                conn.Dispose()
            Catch ex As Exception
                If errorQuery <> vbNullString Then
                    Dim cmd As New SqlCommand
                    cmd.Connection = conn
                    cmd.CommandText = errorQuery
                    cmd.ExecuteNonQuery()
                End If
                result.Add("erro")
            End Try

        Catch ex As Exception
            Module1.fLog("Erro no banco de daados", ex.ToString())
        End Try
        Return result
    End Function

    Public Function insere(ByVal query As String) As Boolean
        Dim conn = New SqlConnection(connectionString)
        Try
            Dim cmd As New SqlCommand
            cmd.Connection = conn
            cmd.CommandText = query
            cmd.ExecuteNonQuery()
            Return True
        Catch ex As Exception
            Return False
        End Try
    End Function
End Class
