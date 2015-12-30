using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.HelpingHand.Implementation;

namespace MongoDB.HelpingHand.Tests
{
    [TestClass]
    public class MongoRepositoryTests
    {
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
        public void GetAll_Should_Find_Records()
        {
            //arrange
            //Customer customer = new Customer();
            MongoRepository<Customer> mongoRepository = new MongoRepository<Customer>("DT003730:27017", "local", "Customer");

            //act
            var customers = mongoRepository.GetAll().Result.ToList();

            //assert
            Assert.IsTrue(customers.Count > 0);
        }

        #endregion
    }
}
