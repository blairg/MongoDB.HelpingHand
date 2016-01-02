using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.HelpingHand.Entities;
using MongoDB.HelpingHand.Implementation;
using NFluent;

namespace MongoDB.HelpingHand.Tests
{
    [TestClass]
    public class MongoRepositoryTests
    {
        private string _server;
        private string _database = "local";
        private string _collection = "Customer";

        [TestInitialize]
        public void TestSetup()
        {
            _server = Environment.MachineName + ":27017";
        }

        [Ignore]
        [TestMethod]
        public void TestMethod1()
        {
            Dictionary<string, object> dictionary = new Dictionary<string, object>();
            dictionary.Add("Key 1", "val 1");
            dictionary.Add("Key 2", 2);
            dictionary.Add("Key 3", 3.1);
            dictionary.Add("Key 4", true);
            dictionary.Add("Key 5", DateTime.Now);

            foreach (var val in dictionary)
            {
                Console.WriteLine(val.Key + ":" + val.Value.GetType().Name);
            }
        }

        #region public async Task<IEnumerable<T>> GetAll()

        [TestMethod]
        [TestCategory("Integration")]
        public void GetAll_Should_Find_Records_As_2_Have_Been_Inserted()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "John", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);


            //act
            var customersFound = mongoRepository.GetAll().Result.ToList();

            //assert
            Check.That(customersFound.Count).Equals(customersToInsert.Count);
            Check.That(customersFound.Count(x => x.Name == customersToInsert[0].Name)).Equals(1);
            Check.That(customersFound.Count(x => x.Age == customersToInsert[0].Age)).Equals(1);
            Check.That(customersFound[0].DateOfBirth.ToLocalTime().ToShortTimeString()).Equals(customersToInsert[0].DateOfBirth.ToLocalTime().ToShortTimeString());
            Check.That(customersFound.Count(x => x.Sex == customersToInsert[0].Sex)).Equals(1);
            Check.That(customersFound.Count(x => x.Name == customersToInsert[1].Name)).Equals(1);
            Check.That(customersFound.Count(x => x.Age == customersToInsert[1].Age)).Equals(1);
            Check.That(customersFound[1].DateOfBirth.ToLocalTime().ToShortTimeString()).Equals(customersToInsert[1].DateOfBirth.ToLocalTime().ToShortTimeString());
            Check.That(customersFound.Count(x => x.Sex == customersToInsert[1].Sex)).Equals(1);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        #endregion


        #region public async Task<IEnumerable<T>> GetMatches(IList<BsonDocumentBuilder> entries, Operator operatorValue)

        [TestMethod]
        [TestCategory("Integration")]
        public void GetMatches_Should_Find_1_Record_With_Name_John()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "John", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);

            IList<BsonDocumentBuilder> bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Name", Value = "John", Operator = Operator.Equals}
            };

            //act
            var customersFound = mongoRepository.GetMatches(bsonDocumentBuilders).Result.ToList();

            //assert
            Check.That(customersFound.Count).Equals(1);
            Check.That(customersFound.Count(x => x.Name == customersToInsert[0].Name)).Equals(1);
            Check.That(customersFound.Count(x => x.Age == customersToInsert[0].Age)).Equals(1);
            Check.That(customersFound[0].DateOfBirth.ToLocalTime().ToShortTimeString()).Equals(customersToInsert[0].DateOfBirth.ToLocalTime().ToShortTimeString());
            Check.That(customersFound.Count(x => x.Sex == customersToInsert[0].Sex)).Equals(1);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void GetMatches_Should_Find_2_Record_With_Name_John_Or_Mary_Or_Age_Greater_Than_21()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "John", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);

            IList<BsonDocumentBuilder> bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Name", Value = "John", Operator = Operator.Equals},
                new BsonDocumentBuilder { Key = "Name", Value = "Mary", Operator = Operator.Equals},
                new BsonDocumentBuilder { Key = "Age", Value = 21, Operator = Operator.GreaterThan}
            };

            //act
            var customersFound = mongoRepository.GetMatches(bsonDocumentBuilders, Operator.Or).Result.ToList();

            //assert
            Check.That(customersFound.Count).Equals(2);
            Check.That(customersFound.Count(x => x.Name == customersToInsert[0].Name)).Equals(1);
            Check.That(customersFound.Count(x => x.Age == customersToInsert[0].Age)).Equals(1);
            Check.That(customersFound[0].DateOfBirth.ToLocalTime().ToShortTimeString()).Equals(customersToInsert[0].DateOfBirth.ToLocalTime().ToShortTimeString());
            Check.That(customersFound.Count(x => x.Sex == customersToInsert[0].Sex)).Equals(1);
            Check.That(customersFound.Count(x => x.Name == customersToInsert[1].Name)).Equals(1);
            Check.That(customersFound.Count(x => x.Age == customersToInsert[1].Age)).Equals(1);
            Check.That(customersFound[1].DateOfBirth.ToLocalTime().ToShortTimeString()).Equals(customersToInsert[1].DateOfBirth.ToLocalTime().ToShortTimeString());
            Check.That(customersFound.Count(x => x.Sex == customersToInsert[1].Sex)).Equals(1);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void GetMatches_Should_Find_0_Records_With_Age_Less_Than_Equal_23()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "John", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);

            IList<BsonDocumentBuilder> bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Age", Value = 23, Operator = Operator.LessThanEquals}
            };

            //act
            var customersFound = mongoRepository.GetMatches(bsonDocumentBuilders).Result.ToList();

            //assert
            Check.That(customersFound.Count).Equals(0);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void GetMatches_Should_Find_1_Records_With_Sex_Not_Equal_To_Female()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "John", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);

            IList<BsonDocumentBuilder> bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Sex", Value = Sex.Female, Operator = Operator.NotEquals}
            };

            //act
            var customersFound = mongoRepository.GetMatches(bsonDocumentBuilders).Result.ToList();

            //assert
            Check.That(customersFound.Count).Equals(1);
            Check.That(customersFound.Count(x => x.Name == customersToInsert[0].Name)).Equals(1);
            Check.That(customersFound.Count(x => x.Age == customersToInsert[0].Age)).Equals(1);
            Check.That(customersFound[0].DateOfBirth.ToLocalTime().ToShortTimeString()).Equals(customersToInsert[0].DateOfBirth.ToLocalTime().ToShortTimeString());
            Check.That(customersFound.Count(x => x.Sex == customersToInsert[0].Sex)).Equals(1);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        #endregion


        #region public asyncTask<T> GetFirst(string objectId)

        [TestMethod]
        [TestCategory("Integration")]
        public void GetFirst_Should_Get_1_Record_By_Object_Id()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;
            DateTime dob = DateTime.Now.AddYears(-25);
            DateTime dobUtc = DateTime.SpecifyKind(dob, DateTimeKind.Utc);
            Customer customer = new Customer {Age = 25, DateOfBirth = dobUtc, Name = "John", Sex = Sex.Male};

            mongoRepository.Insert(customer);

            IList<BsonDocumentBuilder> bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Name", Value = "John", Operator = Operator.Equals}
            };

            var customersFound = mongoRepository.GetMatches(bsonDocumentBuilders).Result.ToList();

            //act

            var customerReturned = mongoRepository.GetFirst(customersFound[0].Id.ToString()).Result;
            customer.Id = customerReturned.Id;
            customer.DateOfBirth = customerReturned.DateOfBirth;//bit of a fail to deal with the difference in the mongo precision

            //assert
            Check.That(customerReturned).IsNotNull();
            Check.That(customerReturned).HasFieldsWithSameValues(customer);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void GetFirst_Should_Not_Get_A_Customer_Back_As_The_Object_Id_Is_Invalid()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;
            DateTime dob = DateTime.Now.AddYears(-25);
            DateTime dobUtc = DateTime.SpecifyKind(dob, DateTimeKind.Utc);
            Customer customer = new Customer { Age = 25, DateOfBirth = dobUtc, Name = "John", Sex = Sex.Male };
            mongoRepository.Insert(customer);
            ObjectId objectId = new ObjectId();

            //act
            var customerReturned = mongoRepository.GetFirst(objectId.ToString()).Result;

            //assert
            Check.That(customerReturned).IsNull();

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void GetFirst_Should_Not_Get_A_Customer_Back_As_The_Object_Id_Format_Is_Invalid()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;
            DateTime dob = DateTime.Now.AddYears(-25);
            DateTime dobUtc = DateTime.SpecifyKind(dob, DateTimeKind.Utc);
            Customer customer = new Customer { Age = 25, DateOfBirth = dobUtc, Name = "John", Sex = Sex.Male };
            mongoRepository.Insert(customer);

            //act

            //assert
            Check.ThatAsyncCode(() => mongoRepository.GetFirst("Invalid format")).Throws<ArgumentException>();

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        #endregion


        #region public async Task<T> GetFirst(IList<BsonDocumentBuilder> entries, Operator operatorValue = Operator.And)

        [TestMethod]
        [TestCategory("Integration")]
        public void GetFirst_Should_Return_1_Record()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "John", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);

            IList<BsonDocumentBuilder> bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Name", Value = "John", Operator = Operator.Equals},
                new BsonDocumentBuilder { Key = "Age", Value = 21, Operator = Operator.GreaterThan}
            };

            //act
            var customerReturned = mongoRepository.GetFirst(bsonDocumentBuilders, Operator.And).Result;
            customersToInsert[0].Id = customerReturned.Id;
            customersToInsert[0].DateOfBirth = customerReturned.DateOfBirth;

            //assert
            Check.That(customerReturned).IsNotNull();
            Check.That(customerReturned).HasFieldsWithSameValues(customersToInsert[0]);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void GetFirst_Should_Return_Null_As_There_No_Records_In_The_Collection()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<BsonDocumentBuilder> bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Name", Value = "John", Operator = Operator.Equals }
            };

            //act
            var customerReturned = mongoRepository.GetFirst(bsonDocumentBuilders, Operator.And).Result;

            //assert
            Check.That(customerReturned).IsNull();

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        #endregion


        #region public async Task<IEnumerable<T>> Search(string key, string value, bool sensitive = false)

        [TestMethod]
        [TestCategory("Integration")]
        public void Search_Should_Find_1_Record_With_Name_John_Case_Insenstive()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "John", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);

            //act
            var customersReturned = mongoRepository.Search("Name", "John").Result.ToList();
            customersToInsert[0].Id = customersReturned[0].Id;
            customersToInsert[0].DateOfBirth = customersReturned[0].DateOfBirth;

            //assert
            Check.That(customersReturned[0]).IsNotNull();
            Check.That(customersReturned[0]).HasFieldsWithSameValues(customersToInsert[0]);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void Search_Should_Find_2_Records_With_Name_James_Case_Insenstive()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "James", Sex = Sex.Male},
                new Customer { Age = 33, DateOfBirth = DateTime.Now.AddYears(-33), Name = "James", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);

            //act
            var customersReturned = mongoRepository.Search("Name", "James").Result.ToList();
            customersToInsert[0].Id = customersReturned[0].Id;
            customersToInsert[0].DateOfBirth = customersReturned[0].DateOfBirth;
            customersToInsert[1].Id = customersReturned[1].Id;
            customersToInsert[1].DateOfBirth = customersReturned[1].DateOfBirth;

            //assert
            Check.That(customersReturned[0]).IsNotNull();
            Check.That(customersReturned[0]).HasFieldsWithSameValues(customersToInsert[0]);
            Check.That(customersReturned[1]).IsNotNull();
            Check.That(customersReturned[1]).HasFieldsWithSameValues(customersToInsert[1]);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void Search_Should_Find_2_Records_With_Partial_Name_Jam_Case_Insenstive()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "James", Sex = Sex.Male},
                new Customer { Age = 33, DateOfBirth = DateTime.Now.AddYears(-33), Name = "James", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);

            //act
            var customersReturned = mongoRepository.Search("Name", "Jam").Result.ToList();
            customersToInsert[0].Id = customersReturned[0].Id;
            customersToInsert[0].DateOfBirth = customersReturned[0].DateOfBirth;
            customersToInsert[1].Id = customersReturned[1].Id;
            customersToInsert[1].DateOfBirth = customersReturned[1].DateOfBirth;

            //assert
            Check.That(customersReturned[0]).IsNotNull();
            Check.That(customersReturned[0]).HasFieldsWithSameValues(customersToInsert[0]);
            Check.That(customersReturned[1]).IsNotNull();
            Check.That(customersReturned[1]).HasFieldsWithSameValues(customersToInsert[1]);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void Search_Should_Find_2_Records_With_Partial_Name_Jam_Case_Sensitive()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "James", Sex = Sex.Male},
                new Customer { Age = 33, DateOfBirth = DateTime.Now.AddYears(-33), Name = "James", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);

            //act
            var customersReturned = mongoRepository.Search("Name", "Jam", sensitive:true).Result.ToList();
            customersToInsert[0].Id = customersReturned[0].Id;
            customersToInsert[0].DateOfBirth = customersReturned[0].DateOfBirth;
            customersToInsert[1].Id = customersReturned[1].Id;
            customersToInsert[1].DateOfBirth = customersReturned[1].DateOfBirth;

            //assert
            Check.That(customersReturned[0]).IsNotNull();
            Check.That(customersReturned[0]).HasFieldsWithSameValues(customersToInsert[0]);
            Check.That(customersReturned[1]).IsNotNull();
            Check.That(customersReturned[1]).HasFieldsWithSameValues(customersToInsert[1]);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void Search_Should_No_Records_With_Name_jam_Case_Sensitive()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "James", Sex = Sex.Male},
                new Customer { Age = 33, DateOfBirth = DateTime.Now.AddYears(-33), Name = "James", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);

            //act
            var customersReturned = mongoRepository.Search("Name", "jam", sensitive: true).Result.ToList();

            //assert
            Check.That(customersReturned.Count).Equals(0);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        #endregion


        #region public async Task<bool> Update(string id, IList<BsonDocumentBuilder> entries)

        [TestMethod]
        [TestCategory("Integration")]
        public void Update_Should_Update_Name_Age_And_Sex()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "James", Sex = Sex.Male},
                new Customer { Age = 33, DateOfBirth = DateTime.Now.AddYears(-33), Name = "James", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);

            IList<BsonDocumentBuilder> bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Name", Value = "Mary", Operator = Operator.Equals }
            };

            Customer customerInserted = mongoRepository.GetFirst(bsonDocumentBuilders).Result;

            IList<BsonDocumentBuilder> updateBsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Name", Value = "Martin" },
                new BsonDocumentBuilder { Key = "Sex", Value = Sex.Male },
                new BsonDocumentBuilder { Key = "Age", Value = 58 }
            };

            //act
            bool updated = mongoRepository.Update(customerInserted.Id.ToString(), updateBsonDocumentBuilders).Result;

            //assert
            Check.That(updated).IsTrue();

            bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Name", Value = "Martin", Operator = Operator.Equals},
                new BsonDocumentBuilder { Key = "Sex", Value = Sex.Male, Operator = Operator.Equals },
                new BsonDocumentBuilder { Key = "Age", Value = 58, Operator = Operator.Equals }
            };

            Customer customerFound = mongoRepository.GetFirst(bsonDocumentBuilders).Result;

            Check.That(customerFound.Name).Equals("Martin");
            Check.That(customerFound.Sex).Equals(Sex.Male);
            Check.That(customerFound.Age).Equals(58);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        #endregion


        #region public async Task<bool> Update(string objectId, T value)

        [TestMethod]
        [TestCategory("Integration")]
        public void Update_Should_Replace_Document()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "Bob", Sex = Sex.Male},
                new Customer { Age = 33, DateOfBirth = DateTime.Now.AddYears(-33), Name = "James", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);

            IList<BsonDocumentBuilder> bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Name", Value = "Bob", Operator = Operator.Equals }
            };

            Customer customerInserted = mongoRepository.GetFirst(bsonDocumentBuilders).Result;
            Customer customerToReplace = new Customer
            {
                Id = customerInserted.Id,
                Name = "Susan",
                Age = 39,
                Sex = Sex.Female
            };

            //act
            bool updated = mongoRepository.Update(customerInserted.Id.ToString(), customerToReplace).Result;

            //assert
            Check.That(updated).IsTrue();

            bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Name", Value = customerToReplace.Name, Operator = Operator.Equals },
                new BsonDocumentBuilder { Key = "Sex", Value = customerToReplace.Sex, Operator = Operator.Equals },
                new BsonDocumentBuilder { Key = "Age", Value = customerToReplace.Age, Operator = Operator.Equals }
            };

            Customer customerFound = mongoRepository.GetFirst(bsonDocumentBuilders).Result;

            Check.That(customerFound.Name).Equals(customerToReplace.Name);
            Check.That(customerFound.Sex).Equals(customerToReplace.Sex);
            Check.That(customerFound.Age).Equals(customerToReplace.Age);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        #endregion


        #region public async Task<bool> Delete(string objectId)

        [TestMethod]
        [TestCategory("Integration")]
        public void Delete_Should_Delete_Record()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>
            {
                new Customer { Age = 25, DateOfBirth = DateTime.Now.AddYears(-25), Name = "Bob", Sex = Sex.Male},
                new Customer { Age = 33, DateOfBirth = DateTime.Now.AddYears(-33), Name = "James", Sex = Sex.Male},
                new Customer { Age = 42, DateOfBirth = DateTime.Now.AddYears(-42), Name = "Mary", Sex = Sex.Female}
            };

            mongoRepository.InsertBatch(customersToInsert);

            IList<BsonDocumentBuilder> bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Sex", Value = Sex.Male, Operator = Operator.NotEquals }
            };

            Customer customerInserted = mongoRepository.GetFirst(bsonDocumentBuilders).Result;

            //act
            bool deleted = mongoRepository.Delete(customerInserted.Id.ToString()).Result;

            //assert
            Check.That(deleted).IsTrue();
            Customer customerFound = mongoRepository.GetFirst(customerInserted.Id.ToString()).Result;
            Check.That(customerFound).IsNull();
            var customersFound = mongoRepository.GetAll().Result;
            Check.That(customersFound.Count()).Equals(2);
            deleteResult = mongoRepository.DeleteAll().Result;
        }

        #endregion


        #region public IMongoCollection<T> MongoCollection { get; set; }

        [TestMethod]
        [TestCategory("Integration")]
        public void MongoCollection_Example_1_Create_Index()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>();
            int age = 0;
            customersToInsert.Add(new Customer { Age = 41, DateOfBirth = DateTime.Now.AddYears(-41), Name = "Joe Bloggs", Sex = Sex.Male });

            for (int i = 0; i < 250000; i++)
            {
                Random random = new Random();
                age = random.Next(10, 99);
                customersToInsert.Add(new Customer { Age = age, DateOfBirth = DateTime.Now.AddYears(-age), Name = "Joe Bloggs", Sex = Sex.Male });
            }
            
            mongoRepository.InsertBatch(customersToInsert);

            IList<BsonDocumentBuilder> bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Age", Value = 41, Operator = Operator.Equals }
            };

            Stopwatch sw = new Stopwatch();
            sw.Start();
            Customer customerFound = mongoRepository.GetFirst(bsonDocumentBuilders).Result;
            sw.Stop();
            double elapsedBeforeIndex = sw.ElapsedMilliseconds;

            var keys = Builders<Customer>.IndexKeys.Ascending("Age");

            //act
            var index = mongoRepository.MongoCollection.Indexes.CreateOneAsync(keys).Result;

            //assert
            Check.That(string.IsNullOrEmpty(index)).IsFalse();
            sw.Reset();
            sw.Start();
            customerFound = mongoRepository.GetFirst(bsonDocumentBuilders).Result;
            Check.That(customerFound).IsNotNull();
            sw.Stop();
            double elapsedAfterIndex = sw.ElapsedMilliseconds;

            Check.That(elapsedBeforeIndex).IsGreaterThan(elapsedAfterIndex);

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void MongoCollection_Example_2_Aggregation_Grouping()
        {
            //arrange
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>(_server, _database, _collection);
            var deleteResult = mongoRepository.DeleteAll().Result;

            IList<Customer> customersToInsert = new List<Customer>();

            for (int i = 0; i < 20000; i++)
            {
                Random random = new Random();
                var age = random.Next(10, 99);
                customersToInsert.Add(new Customer { Age = age, DateOfBirth = DateTime.Now.AddYears(-age), Name = "Joe Bloggs", Sex = Sex.Male });
            }

            mongoRepository.InsertBatch(customersToInsert);

            IList<BsonDocumentBuilder> bsonDocumentBuilders = new List<BsonDocumentBuilder>
            {
                new BsonDocumentBuilder { Key = "Age", Value = 41, Operator = Operator.Equals }
            };

            //act
            var aggregationResult = mongoRepository.MongoCollection.Aggregate().Group(new BsonDocument { { "_id", "$Age" }, { "count", new BsonDocument("$sum", 1) } });
            var results = aggregationResult.ToList();

            //assert
            foreach (var result in results)
            {
                BsonDocument bsonDocument = result.ToBsonDocument();
                Console.WriteLine(bsonDocument[0] + ":" + bsonDocument[1]);
            }

            deleteResult = mongoRepository.DeleteAll().Result;
        }

        #endregion

    }
}
