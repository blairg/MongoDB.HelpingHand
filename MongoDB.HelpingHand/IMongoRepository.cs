using System;
using System.Collections.Generic;

namespace MongoDB.HelpingHand
{
    public interface IMongoRepository<T> : IDisposable
    {
        //MongoCollection<T> GetCollection();

        IEnumerable<T> GetAll();

        IEnumerable<T> GetMatches(string key, string value);

        IEnumerable<T> GetMatches(string key, double value);

        IEnumerable<T> GetMatches(string key, DateTime value);

        IEnumerable<T> GetMatches(string key, bool value);

        /// <summary>
        /// Get document by ObjectId
        /// </summary>
        /// <param name="objectId">Id of document</param>
        /// <returns>Document</returns>
        T GetFirst(string objectId);

        T GetFirst(string key, string value);

        T GetFirst(string key, double value);

        T GetFirst(string key, DateTime value);

        T GetFirst(string key, bool value);

        IEnumerable<T> Search(string key, string value, bool sensitive = false);

        // Creates a Task and inserts it into the collection in MongoDB.
        T Insert(T value, bool acknowledge = true);

        // Creates a Task and inserts it into the collection in MongoDB.
        IEnumerable<T> InsertBatch(IEnumerable<T> values, bool acknowledge = true);

        // Full update of a document
        T Update(T value, bool acknowledge = true);

        // Delete a document
        void Delete(string objectId, bool isObjectId = false);
    }
}
