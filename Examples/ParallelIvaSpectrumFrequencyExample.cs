// This example discovers and connects to the configured instruments and
// runs their spectrum versus frequency measurements in parallel. It streams
// the results to a timestamped CSV file and disconnects all instruments when
// the run completes.
#:package Kaelus.Unify3Sdk@0.2.0
using Kaelus.Unify3Sdk;
using System.Collections.Concurrent;
using System.Globalization;

var unify = Unify.Instance;

List<InstrumentConfiguration> instrumentConfigurations =
[
    new("TX2153200147", new Extent { Start = 1_000_000_000, End = 1_498_750_000 }, 400),
    new("TX2153800230", new Extent { Start = 1_500_000_000, End = 1_998_750_000 }, 400),
    new("TX2164400038", new Extent { Start = 2_000_000_000, End = 2_500_000_000 }, 401),
];

try
{
    ConcurrentBag<string> connectedInstrumentSerialNumbers = [];
    using CancellationTokenSource resultsCancellation = new();
    Task resultsTask = Task.CompletedTask;

    try
    {
        List<Instrument> instruments = [];

        // Run instrument discovery until all required instruments are found or the timeout expires.
        using CancellationTokenSource discoveryTimeout = new(TimeSpan.FromSeconds(60));
        try
        {
            _ = unify.RunBluetoothScan();

            await foreach (var candidateInstruments in unify.RunInstrumentDiscovery(discoveryTimeout.Token))
            {
                List<Instrument> newInstruments =
                [..
                    from i in candidateInstruments
                    where instrumentConfigurations.Any(configuration => configuration.InstrumentSerialNumber == i.SerialNumber) &&
                        !instruments.Any(existing => existing.SerialNumber == i.SerialNumber)
                    select i
                ];
                foreach (var newInstrument in newInstruments)
                {
                    Console.WriteLine($"Discovered instrument: {newInstrument.SerialNumber}");
                }
                instruments.AddRange(newInstruments);
                if (instruments.Count == instrumentConfigurations.Count)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (discoveryTimeout.IsCancellationRequested)
        {
            throw new System.TimeoutException("Timed out waiting for instruments to be discovered.");
        }
        finally
        {
            await unify.StopBluetoothScan();
        }

        // Connect to all instruments in parallel.
        await Task.WhenAll(instruments.Select(async instrument =>
        {
            Console.WriteLine($"Connecting to instrument: {instrument.SerialNumber}");
            var connectionResult = await unify.ConnectInstrument(instrument.SerialNumber);
            if (connectionResult != ConnectionResult.Success)
            {
                throw new InvalidOperationException($"Connecting to {instrument.SerialNumber} failed with result {connectionResult}.");
            }
            connectedInstrumentSerialNumbers.Add(instrument.SerialNumber);

            // Get the instrument definition to determine the exact model of the connected instrument.
            if (unify.GetInstrumentDefinition(instrument.SerialNumber) is not CaaInstrumentDefinition instrumentDefinition)
            {
                throw new InvalidOperationException("The connected instrument did not provide a CAA instrument definition.");
            }
            instrument.Model = instrumentDefinition.Model;
        }));

        // Create a test that uses the instruments.
        Test test = new()
        {
            Traces =
            [..
                from configuration in instrumentConfigurations
                let instrument = instruments.Single(i => i.SerialNumber == configuration.InstrumentSerialNumber)
                select new CaaSpectrumFrequencyTrace
                {
                    FrequencyUnit = FrequencyUnit.Hertz,
                    PowerUnit = PowerUnit.Dbm,
                    PreampMode = CaaPreampMode.Auto,
                    InstrumentSerialNumber = instrument.SerialNumber,
                    InstrumentModel = instrument.Model,
                    FrequencyRange_Hz = configuration.FrequencyRange_Hz,
                    NumberOfPoints = configuration.NumberOfPoints,
                    Duration_s = 30,
                }
            ],
            Cardinality = Cardinality.Many,
            Ordering = Ordering.Parallel,
        };

        // Configure the test before running it so the SDK can validate the
        // requested traces against the connected instruments.
        var testConfigurationResult = unify.ConfigureTest(test);

        if (!testConfigurationResult.Valid)
        {
            throw new InvalidOperationException("The SDK rejected the test configuration.");
        }

        // Read and record batched results while the instruments execute the test.
        string resultsPath = Path.GetFullPath($"spectrum-results-{DateTime.UtcNow:yyyyMMddTHHmmssfffZ}.csv");
        Console.WriteLine($"Writing spectrum results to: {resultsPath}");

        resultsTask = Task.Run(async () =>
        {
            await using StreamWriter resultsWriter = new(resultsPath, append: false);
            await resultsWriter.WriteLineAsync("instrument,time,frequency,power");
            try
            {
                await foreach (var batchedTestResult in unify.GetBatchedTestResults(resultsCancellation.Token))
                {
                    string timestamp = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);
                    string instrumentSerialNumber = instrumentConfigurations[batchedTestResult.TraceIndex].InstrumentSerialNumber;
                    foreach (var point in batchedTestResult.TracePoints)
                    {
                        await resultsWriter.WriteLineAsync(string.Join(",",
                            instrumentSerialNumber,
                            timestamp,
                            point.Values[0].ToString("R", CultureInfo.InvariantCulture),
                            point.Values[1].ToString("R", CultureInfo.InvariantCulture)));
                    }
                }
            }
            catch (OperationCanceledException) when (resultsCancellation.IsCancellationRequested)
            {
            }
        });

        Console.WriteLine("Starting test");

        // Run the test and stream progress updates while the instruments execute the test.
        double progressReportInterval = 0.05;
        double nextProgressReport = progressReportInterval;
        await foreach (var progressUpdate in unify.RunTest(checkRl: true, CancellationToken.None))
        {
            if (progressUpdate.TraceProgress >= nextProgressReport)
            {
                var percentage = (int)Math.Round(progressUpdate.TraceProgress * 100);
                Console.WriteLine($"Progress: {percentage}%");
                nextProgressReport += progressReportInterval;
            }
        }

        Console.WriteLine("Test complete!");
    }
    finally
    {
        resultsCancellation.Cancel();
        await resultsTask;
        foreach (string instrumentSerialNumber in connectedInstrumentSerialNumbers)
        {
            await unify.DisconnectInstrument(instrumentSerialNumber);
        }
    }
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Test failed: {ex.Message}");
    Environment.ExitCode = 1;
}

record InstrumentConfiguration(string InstrumentSerialNumber, Extent FrequencyRange_Hz, int NumberOfPoints);