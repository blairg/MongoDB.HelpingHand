using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.HelpingHand.Entities;

namespace MongoDB.HelpingHand
{
    public interface IMongoRepository<T>
    {
        Task<IEnumerable<T>> GetAll();
        Task<IEnumerable<T>> GetMatches(IList<BsonDocumentBuilder> entries, Operator operatorValue);
        Task<T> GetFirst(string objectId);
        Task<T> GetFirst(IList<BsonDocumentBuilder> entries, Operator operatorValue);
        Task<IEnumerable<T>> Search(string key, string value, bool sensitive = false);
        void Insert(T value);
        void InsertBatch(IEnumerable<T> values);
        Task<bool> Update(string id, IList<BsonDocumentBuilder> entries);
        Task<bool> Update(string id, T value);
        Task<bool> Delete(string id);
    }
}
