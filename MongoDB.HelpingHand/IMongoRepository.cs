using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Driver;
using MongoDB.HelpingHand.Entities;

namespace MongoDB.HelpingHand
{
    public interface IMongoRepository<T>
    {
        IMongoClient MongoClient
        {
            get;
            set;
        }

        IMongoCollection<T> MongoCollection
        {
            get;
            set;
        }

        /// <summary>
        /// Gets all documents in the collection of type T.
        /// </summary>
        /// <returns></returns>
        Task<IEnumerable<T>> GetAll();

        /// <summary>
        /// Gets all matches which match the keys and values passed.
        /// </summary>
        /// <param name="entries"></param>
        /// <param name="operatorValue">Must be And or Or</param>
        /// <returns></returns>
        Task<IEnumerable<T>> GetMatches(IList<BsonDocumentBuilder> entries, Operator operatorValue = Operator.And);

        /// <summary>
        /// Gets the first document by it's ObjectId.
        /// </summary>
        /// <param name="objectId"></param>
        /// <returns></returns>
        Task<T> GetFirst(string objectId);

        /// <summary>
        /// Get first match which matches the keys and values passed.
        /// </summary>
        /// <param name="entries"></param>
        /// <param name="operatorValue">Must be And Or Or</param>
        /// <returns></returns>
        Task<T> GetFirst(IList<BsonDocumentBuilder> entries, Operator operatorValue = Operator.And);

        /// <summary>
        /// Performs a regular expression search against a single column. Case be sensitive or not.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="sensitive">True for case-senstive and false (default) if not.</param>
        /// <returns>List of T</returns>
        Task<IEnumerable<T>> Search(string key, string value, bool sensitive = false);

        /// <summary>
        /// Inserts a single document
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        void Insert(T value);

        /// <summary>
        /// Inserts a batch of documents
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        void InsertBatch(IEnumerable<T> values);

        /// <summary>
        /// Partial update of a document
        /// </summary>
        /// <param name="objectId"></param>
        /// <param name="entries">List of all fields which require updating</param>
        /// <returns></returns>
        Task<bool> Update(string objectId, IList<BsonDocumentBuilder> entries);

        /// <summary>
        /// Full update of a document
        /// </summary>
        /// <param name="objectId"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        Task<bool> Update(string objectId, T value);

        /// <summary>
        /// Deletes all documents in the collection.
        /// </summary>
        /// <returns></returns>
        Task<bool> DeleteAll();

        /// <summary>
        /// Delete a document
        /// </summary>
        /// <param name="objectId"></param>
        Task<bool> Delete(string objectId);

        /// <summary>
        /// Creates an index on a document.
        /// </summary>
        /// <param name="columnName">Column to create index on</param>
        /// <returns></returns>
        Task<string> CreateIndex(string columnName);
    }
}
