using System;
using System.Collections.Generic;
using BattleSpy.Database;
using MySql.Data.MySqlClient;
using BattleSpy;

namespace Server.Database
{
    /// <summary>
    /// A class to provide common tasks against the Gamespy Login Database
    /// </summary>
    public sealed class GamespyDatabase : DatabaseDriver, IDisposable
    {
        /// <summary>
        /// Our Database connection parameters
        /// </summary>
        private static MySqlConnectionStringBuilder Builder;

        /// <summary>
        /// For locking while creating a new account
        /// </summary>
        private static Object _sync = new Object();

        /// <summary>
        /// Builds the conenction string statically, and just once
        /// </summary>
        static GamespyDatabase()
        {
            Builder = new MySqlConnectionStringBuilder();
            Builder.Server = Config.GetValue("Database", "Hostname");
            Builder.Port = Config.GetType<uint>("Database", "Port");
            Builder.UserID = Config.GetValue("Database", "Username");
            Builder.Password = Config.GetValue("Database", "Password");
            Builder.Database = Config.GetValue("Database", "LoginDatabase");
            Builder.ConvertZeroDateTime = true;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public GamespyDatabase() : base("Mysql", Builder.ConnectionString)
        {
            // Try and Reconnect
            base.Connect();
        }

        /// <summary>
        /// Destructor
        /// </summary>
        ~GamespyDatabase()
        {
            if (!base.IsDisposed)
                base.Dispose();
        }

        /// <summary>
        /// Fetches an account from the gamespy database
        /// </summary>
        /// <param name="Nick">The user's Nick</param>
        /// <returns></returns>
        public Dictionary<string, object> GetUser(string Nick)
        {
            // Fetch the user
            var Rows = base.Query("SELECT pid, email, password, game_country FROM web_users WHERE username=@P0", Nick);
            return (Rows.Count == 0) ? null : Rows[0];
        }

        /// <summary>
        /// Fetches an account from the gamespy database
        /// </summary>
        /// <param name="Email">Account email</param>
        /// <param name="Password">Account Password Un-Hashed</param>
        /// <returns></returns>
        public List<Dictionary<string, object>> GetUsersByEmailPass(string Email, string Password)
        {
            return base.Query("SELECT * FROM web_users WHERE LOWER(email)=@P0 AND password=@P1", Email.ToLowerInvariant(), Password.GetMD5Hash());
        }

        /// <summary>
        /// Returns wether an account exists from the provided Nick
        /// </summary>
        /// <param name="Nick"></param>
        /// <returns></returns>
        public bool UserExists(string Nick)
        {
            // Fetch the user
            return (base.Query("SELECT pid FROM web_users WHERE username=@P0", Nick).Count != 0);
        }

        /// <summary>
        /// Returns wether an account exists from the provided Account Id
        /// </summary>
        /// <param name="PID"></param>
        /// <returns></returns>
        public bool UserExists(int PID)
        {
            // Fetch the user
            return (base.Query("SELECT username FROM web_users WHERE pid=@P0", PID).Count != 0);
        }

        /// <summary>
        /// Creates a new Gamespy Account
        /// </summary>
        /// <remarks>Used by the login server when a create account request is made</remarks>
        /// <param name="Nick">The Account Name</param>
        /// <param name="Pass">The UN-HASHED Account Password</param>
        /// <param name="Email">The Account Email Address</param>
        /// <param name="Country">The Country Code for this Account</param>
        /// <returns>Returns the Player ID if sucessful, 0 otherwise</returns>
        public int CreateUser(string Nick, string Pass, string Email, string Country)
        {
            // Only allow 1 thread in here at a time to prevent race conditions
            lock (_sync)
            {
                int Pid = 0;

                // Attempt to connect to stats database, and get a PID from there
                try
                {
                    // try see if the player ID exists in the stats database
                    // NOTE: online account names in the stats DB start with a single space!
                    var Row = base.Query("SELECT pid FROM player WHERE upper(name) = upper(@P0)", " {Nick}");
                    Pid = (Row.Count == 0) ? GenerateAccountId() : Int32.Parse(Row[0]["pid"].ToString());
                }
                catch
                {
                    Pid = GenerateAccountId();
                }

                // Create the user in the database
                int Rows = base.Execute("INSERT INTO web_users(pid, username, password, email, country) VALUES(@P0, @P1, @P2, @P3, @P4)",
                    Pid, Nick, Pass.GetMD5Hash(false), Email.ToLowerInvariant(), Country
                );

                return (Rows != 0) ? Pid : 0;
            }
        }

        /// <summary>
        /// Generates a new Account Id
        /// </summary>
        /// <returns></returns>
        private int GenerateAccountId()
        {
            int max = base.ExecuteScalar<int>("SELECT COALESCE(MAX(pid), 500000000) FROM web_users") + 1;
            return (max < 500000000) ? 500000000 : max;
        }

        /// <summary>
        /// Updates an Accounts Country Code
        /// </summary>
        /// <param name="Nick"></param>
        /// <param name="Country"></param>
        public void UpdateUser(string Nick, string Country)
        {
            base.Execute("UPDATE web_users SET game_country=@P0 WHERE username=@P1", Nick, Country);
        }

        /// <summary>
        /// Updates an Account's information by ID
        /// </summary>
        /// <param name="Id">The Current Account ID</param>
        /// <param name="NewPid">New Account ID</param>
        /// <param name="NewNick">New Account Name</param>
        /// <param name="NewPassword">New Account Password that is UN-Hashed</param>
        /// <param name="NewEmail">New Account Email Address</param>
        public void UpdateUser(int Id, int NewPid, string NewNick, string NewPassword, string NewEmail)
        {
            base.Execute("UPDATE web_users SET pid=@P0, username=@P1, password=@P2, email=@P3 WHERE pid=@P4", 
                NewPid, NewNick, NewPassword.GetMD5Hash(), NewEmail.ToLowerInvariant(), Id);
        }

        /// <summary>
        /// Deletes a Gamespy Account
        /// </summary>
        /// <param name="Nick"></param>
        /// <returns></returns>
        public int DeleteUser(string Nick)
        {
            return base.Execute("DELETE FROM web_users WHERE username=@P0", Nick);
        }

        /// <summary>
        /// Deletes a Gamespy Account
        /// </summary>
        /// <param name="Nick"></param>
        /// <returns></returns>
        public int DeleteUser(int Pid)
        {
            return base.Execute("DELETE FROM web_users WHERE pid=@P0", Pid);
        }

        /// <summary>
        /// Fetches a Gamespy Account id from an account name
        /// </summary>
        /// <param name="Nick"></param>
        /// <returns></returns>
        public int GetPlayerId(string Nick)
        {
            var Rows = base.Query("SELECT pid FROM web_users WHERE username=@P0", Nick);
            return (Rows.Count == 0) ? 0 : Int32.Parse(Rows[0]["pid"].ToString());
        }

        /// <summary>
        /// Sets the Account (Player) Id for an account by Name
        /// </summary>
        /// <param name="Nick">The account Nick we are setting the new Pid for</param>
        /// <param name="Pid">The new Pid</param>
        /// <returns></returns>
        public int SetPID(string Nick, int Pid)
        {
            // If no user exists, return code -1
            if (!UserExists(Nick))
                return -1;

            // If the Pid already exists, return -2
            if (UserExists(Pid))
                return -2;

            // If PID is false, the PID is not taken
            return base.Execute("UPDATE web_users SET pid=@P0 WHERE username=@P1", Pid, Nick);
        }

        /// <summary>
        /// Returns the number of accounts in the database
        /// </summary>
        /// <returns></returns>
        public int GetNumAccounts()
        {
            return base.ExecuteScalar<int>("SELECT COUNT(pid) FROM web_users");
        }
    }
}
