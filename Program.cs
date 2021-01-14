using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConnectionTo_PosgerSQL
{
    class Program
    {
        static  bool TestDbConnection (string conn)
        {
            try
            {
                using (var con = new NpgsqlConnection(conn))
                {
                    con.Open();
                    return true;
                }
                    
            }

            catch (Exception ex)
            {
                // write error to log file
                return false;
            }
        }

        static void PrintAllMovies (string conn_string)
        {
            using (var con = new NpgsqlConnection(conn_string))
            {
                con.Open();
                string query = "Select * from movies";


                NpgsqlCommand command = new NpgsqlCommand(query, con);
                command.CommandType = System.Data.CommandType.Text;//this is default

                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    long id = (long)reader["id"];
                    string title = (string)reader["title"];
                    //......
                    Console.WriteLine($"{id} {title}")
                    };
            }
        }

        private static int Run_sp_GetRandomNumber(string conn_string, int limit)
        {
            try 
            {
                using (var con = new NpgsqlConnection(conn_string))
                {
                    con.Open();
                    string sp_name = "a_sp_get_randoms";//"call procedure_name(a,b,c...)" <- אם פונקציה לא מחזירה ערך אז לקרוא לה בצורה הבאה

                    NpgsqlCommand command = new NpgsqlCommand(sp_name, con);
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    command.Parameters.AddRange(new NpgsqlParameter[]
                        {
                            new NpgsqlParameter("max",limit)
                        });
                    var reader = command.ExecuteReader();
                    if (reader.Read())
                    {
                        int random_number = (int)reader["a_sp_get_randoms"];

                        Console.WriteLine($"{random_number} ");
                        return random_number;

                    };

                    throw new ApplicationException("Function not return value");
                }
            }
            
            catch (Exception ex)
            {
                Console.WriteLine($"Function a_sp_get_randoms failed. Parameters: {limit}");
                return 0;
            }

        }

        static void Main(string[] args)
        {
            //Read from congig file
            string conn_string = "Host = localhost;Username=postgres;Password=admin;Database=posgtres";

            // pered tem kak sistema podnimaetca nugno proverit podkuchenie
           if ( TestDbConnection(conn_string))
            {
                PrintAllMovies(conn_string);
                int res = Run_sp_GetRandomNumber(conn_string,100);

            }

           else
            {
                Console.WriteLine("Cannot connect to DB!!!");
            }
        }
    }
}
