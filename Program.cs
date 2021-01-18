using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyPostgres
{
    class Program
    {
        static bool TestDbConnection(string conn)
        {
            try
            {
                using (var my_conn = new NpgsqlConnection(conn))
                {
                    my_conn.Open();
                    return true;
                }
            }
            catch (Exception ex)
            {
                // write error to log file
                return false;
            }
        }

        // Функция для обычного запроса для БД
        private static void PrintAllMovies(string conn_string)
        {
            using (var conn = new NpgsqlConnection(conn_string))
            {
                conn.Open();
                string query = "SELECT * FROM movies";

                NpgsqlCommand command = new NpgsqlCommand(query, conn);
                command.CommandType = System.Data.CommandType.Text; // this is default

                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    /* Можно сделать отдельный класс ПОКО для конкретного запроса. Можно сделать анонимный объект,
                        * а можно прямо присвоить значение полям, как показано ниже:**/

                    long id = (long)reader["id"];
                    string title = (string)reader["title"];
                    // ....
                    Console.WriteLine($"{id} {title}");
                }
            }
        }

        // Функция запускающая СТОР ПОСИЖЕР с параметром
        private static int Run_sp_GetRandomNumber(string conn_string, int limit)
        {
            try
            {
                using (var conn = new NpgsqlConnection(conn_string))
                {
                    conn.Open();
                    string sp_name = "a_sp_get_randoms";// вместо запроса вписываем имя СТОР ПОСИЖЕР
                    // "call procedure_name(a,b,c...) -= > Такая форма пишется в случае если СТОР ПОСИЖЕР ничего не возваращает"

                    NpgsqlCommand command = new NpgsqlCommand(sp_name, conn);
                    command.CommandType = System.Data.CommandType.StoredProcedure; //Ставим СТОР ПОСИЖЕР как тип команды (не ТЕКСТ как в обычном запросе

                    // форма передачи параметра в СП
                    command.Parameters.AddRange(new NpgsqlParameter[]
                    { 
                       new NpgsqlParameter("_max", limit) // в скобках указывается имя параметра и его значение
                    });

                    var reader = command.ExecuteReader();
                    if (reader.Read()) // здесь нет нужды использовать WHILE т.к. возвращается только единственный результат а не несколько.
                    {
                        int random_number = (int)reader["a_sp_get_randoms"];// в скобках пишется не название переменной, а имя СП;
                        // ....
                        return random_number;
                    }
                    throw new ApplicationException("Function not returned value!");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                Console.WriteLine($"Function a_sp_get_randoms failed. parameters: {limit}");
                return 0;
            }
        }
        private static int Run_sp_GetMax(string conn_string, int x, int y)
        {
            try
            {
                using (var conn = new NpgsqlConnection(conn_string))
                {
                    conn.Open();
                    string sp_name = "a_sp_max";

                    NpgsqlCommand command = new NpgsqlCommand(sp_name, conn);
                    command.CommandType = System.Data.CommandType.StoredProcedure; // this is default

                    command.Parameters.AddRange(new NpgsqlParameter[]
                    {
                    new NpgsqlParameter("x", x),
                    new NpgsqlParameter("y", y),
                    });

                    var reader = command.ExecuteReader();
                    if (reader.Read())
                    {
                        int random_number = (int)reader["a_sp_max"];
                        // ....
                        return random_number;
                    }
                    throw new ApplicationException("Function not returned value!");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                Console.WriteLine($"Function a_sp_max failed. parameters: x:{x} y:{y}");
                return 0;
            }
        }

        /* Ниже приведена генерическая функция, кот принимает "conn_string", имя СП и его параметры ввиде массива параметров.
         * List<Dictionary<string, object> -- лист получает название поля и его значение как объект
         */
        private static List<Dictionary<string, object>> Run_sp(string conn_string, string sp_name,
            NpgsqlParameter[] parameters)
        {
            List<Dictionary<string, object>> items = new List<Dictionary<string, object>>();

            try
            {
                using (var conn = new NpgsqlConnection(conn_string))
                {
                    conn.Open();

                    NpgsqlCommand command = new NpgsqlCommand(sp_name, conn);
                    command.CommandType = System.Data.CommandType.StoredProcedure; // this is default

                    //используя AddRange мы можем сразу передать СП полученый массив параметров
                    command.Parameters.AddRange(parameters);

                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        Dictionary<string, object> one_row = new Dictionary<string, object>();
                        foreach (var item in reader.GetColumnSchema())
                        {
                            object column_value = reader[item.ColumnName];
                            one_row.Add(item.ColumnName, column_value);
                        }
                        items.Add(one_row);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                Console.WriteLine($"Function {sp_name} failed. parameters: {string.Join(",", parameters.Select(_ => _.ParameterName + " : " + _.Value))}");
            }
            return items;
        }
        static void Main(string[] args)
        {
            // Что бы увидеть conn string нужно в Дата Грип правой кнопкой по названию ДБ => Properties
            string conn_string = "Host=localhost;Username=postgres;Password=admin;Database=postgres";

            // on systme startup
            if (TestDbConnection(conn_string))
            {
              
                PrintAllMovies(conn_string);
                int res = Run_sp_GetRandomNumber(conn_string, 100);
                Console.WriteLine($"Random number is: {res}");
                int max = Run_sp_GetMax(conn_string, 70, 89);
                Console.WriteLine($"Max number is: {max}");

                var res_sp_max = Run_sp(conn_string, "a_sp_max", new NpgsqlParameter[]
                {
                    new NpgsqlParameter("x", 70),
                    new NpgsqlParameter("y", 89)
                });
                Console.WriteLine(
                    $"Run sp of a_sp_max. result = {res_sp_max[0]["a_sp_max"]}");

                var res_sp_random = Run_sp(conn_string, "a_sp_get_randoms", new NpgsqlParameter[]
                {
                    new NpgsqlParameter("_max", 100)
                });
                Console.WriteLine(
                    $"Run sp of a_sp_get_randoms. result = {res_sp_random[0]["a_sp_get_randoms"]}");

                var res_sp_movies_mid = Run_sp(conn_string, "a_sp_get_movies_mid", new NpgsqlParameter[]
                {

                });
                Console.WriteLine();
            }
            else
            {
                Console.WriteLine("Cannot connect to db!");
            }
        }


    }
}
