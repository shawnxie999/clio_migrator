#include <backend/BackendFactory.h>
#include <backend/CassandraBackend.h>
#include <config/Config.h>
#include <etl/NFTHelpers.h>
#include <main/Build.h>

#include <boost/asio.hpp>
#include <boost/log/trivial.hpp>
#include <cassandra.h>

#include <iostream>

static std::uint32_t const MAX_RETRIES = 5;
static std::chrono::seconds const WAIT_TIME = std::chrono::seconds(60);

static void
wait(boost::asio::steady_timer& timer, std::string const reason)
{
    BOOST_LOG_TRIVIAL(info) << reason << ". Waiting";
    timer.expires_after(WAIT_TIME);
    timer.wait();
    BOOST_LOG_TRIVIAL(info) << "Done";
}

static const CassResult*
doTryGetTxPageResult(
    CassStatement* const query,
    boost::asio::steady_timer& timer,
    Backend::CassandraBackend& backend,
    std::uint32_t const attempts = 0)
{
    CassFuture* fut = cass_session_execute(backend.cautionGetSession(), query);
    CassResult const* result = cass_future_get_result(fut);
    cass_future_free(fut);

    if (result != nullptr)
        return result;

    if (attempts >= MAX_RETRIES)
        throw std::runtime_error("Already retried too many times");

    wait(timer, "Unexpected empty result from tx paging");
    return doTryGetTxPageResult(query, timer, backend, attempts + 1);
}

static void
doMigration(
    Backend::CassandraBackend& backend,
    boost::asio::steady_timer& timer,
    boost::asio::yield_context& yield)
{
    BOOST_LOG_TRIVIAL(info) << "Beginning migration";
    auto const ledgerRange = backend.hardFetchLedgerRangeNoThrow(yield);

    /*
     * Step 0 - If we haven't downloaded the initial ledger yet, just short
     * circuit.
     */
    if (!ledgerRange)
    {
        BOOST_LOG_TRIVIAL(info) << "There is no data to migrate";
        return;
    }

    int count = 0;

    /*
     * Step 1 - Look at all NFT transactions recorded in
     * `nf_token_transactions` and reload any NFTokenMint transactions. These
     * will contain the URI of any tokens that were minted after our start
     * sequence. We look at transactions for this step instead of directly at
     * the tokens in `nf_tokens` because we also want to cover the extreme
     * edge case of a token that is re-minted with a different URI.
     */
    std::stringstream query;
    query << "SELECT uri FROM " << backend.tablePrefix() << "nf_token_uris";
    CassStatement* nftTxQuery = cass_statement_new(query.str().c_str(), 0);
    cass_statement_set_paging_size(nftTxQuery, 1000);
    cass_bool_t morePages = cass_true;

    // For all NFT txs, paginated in groups of 1000...
    while (morePages)
    {
        std::vector<NFTsData> toWrite;

        CassResult const* result =
            doTryGetTxPageResult(nftTxQuery, timer, backend);

        // For each tx in page...
        CassIterator* txPageIterator = cass_iterator_from_result(result);
        while (cass_iterator_next(txPageIterator))
            count++;

        morePages = cass_result_has_more_pages(result);
        if (morePages)
            cass_statement_set_paging_state(nftTxQuery, result);
        cass_iterator_free(txPageIterator);
        cass_result_free(result);
    }

    cass_statement_free(nftTxQuery);
    BOOST_LOG_TRIVIAL(info) << "\nThere are " << count << " uris\n";

    count = 0;
    query.str("");
    query << "SELECT issuer FROM " << backend.tablePrefix()
          << "issuer_nf_tokens_v2";
    CassStatement* other = cass_statement_new(query.str().c_str(), 0);
    cass_statement_set_paging_size(other, 1000);
    morePages = cass_true;

    // For all NFT txs, paginated in groups of 1000...
    while (morePages)
    {
        CassResult const* result = doTryGetTxPageResult(other, timer, backend);

        // For each tx in page...
        CassIterator* txPageIterator = cass_iterator_from_result(result);
        while (cass_iterator_next(txPageIterator))
            count++;

        morePages = cass_result_has_more_pages(result);
        if (morePages)
            cass_statement_set_paging_state(other, result);
        cass_iterator_free(txPageIterator);
        cass_result_free(result);
    }

    cass_statement_free(other);
    BOOST_LOG_TRIVIAL(info) << "\nThere are " << count << " issuers\n";

    count = 0;
    query.str("");
    query << "SELECT hash FROM " << backend.tablePrefix()
          << "nf_token_transactions";
    CassStatement* another = cass_statement_new(query.str().c_str(), 0);
    cass_statement_set_paging_size(another, 1000);
    morePages = cass_true;

    // For all NFT txs, paginated in groups of 1000...
    while (morePages)
    {
        CassResult const* result =
            doTryGetTxPageResult(another, timer, backend);

        // For each tx in page...
        CassIterator* txPageIterator = cass_iterator_from_result(result);
        while (cass_iterator_next(txPageIterator))
            count++;

        morePages = cass_result_has_more_pages(result);
        if (morePages)
            cass_statement_set_paging_state(other, result);
        cass_iterator_free(txPageIterator);
        cass_result_free(result);
    }

    cass_statement_free(another);
    BOOST_LOG_TRIVIAL(info) << "\nThere are " << count << " txs\n";
}

int
main(int argc, char* argv[])
{
    if (argc < 2)
    {
        std::cerr << "Didn't provide config path!" << std::endl;
        return EXIT_FAILURE;
    }

    std::string const configPath = argv[1];
    auto const config = clio::ConfigReader::open(configPath);
    if (!config)
    {
        std::cerr << "Couldn't parse config '" << configPath << "'"
                  << std::endl;
        return EXIT_FAILURE;
    }

    auto const type = config.value<std::string>("database.type");
    if (!boost::iequals(type, "cassandra"))
    {
        std::cerr << "Migration only for cassandra dbs" << std::endl;
        return EXIT_FAILURE;
    }

    boost::asio::io_context ioc;
    boost::asio::steady_timer timer{ioc};
    auto workGuard = boost::asio::make_work_guard(ioc);
    auto backend = Backend::make_Backend(ioc, config);

    boost::asio::spawn(
        ioc, [&backend, &workGuard, &timer](boost::asio::yield_context yield) {
            doMigration(*backend, timer, yield);
            workGuard.reset();
        });

    ioc.run();
    BOOST_LOG_TRIVIAL(info) << "SUCCESS!";
    return EXIT_SUCCESS;
}
