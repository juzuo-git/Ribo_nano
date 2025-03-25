#!/public/home/lijz/anaconda3/envs/R_envbk/bin/Rscript --vanilla

library(edgeR)
library(optparse)

parser <- OptionParser()
parser <- add_option(parser, c("-i", "--input"), type="character", 
						help="input the reads length and reads count file")
parser <- add_option(parser, c("-o", "--out"), type="character", 
						help="Output File of rpkm")
parser <- add_option(parser, c("-l", "--length_col"), type="character", default = "Length",
						help="Name of the column containing gene lengths")

opt <- parse_args(parser)
file_in = opt$input
length_col_str = opt$length_col
out_file = opt$out

# if (is.null(file_in)) {
#     print_help(parser)
#     stop("Input file must be provided!", call. = FALSE)
# }

data <- read.csv(file_in, row.names = 1, sep = "\t")

count_data <- data[, -which(names(data) == length_col_str)] 
gene_lengths <- data[[length_col_str]] 

dge <- DGEList(counts = count_data, genes = data.frame(Length = gene_lengths))

rpkm_values <- rpkm(dge, gene.length = dge$genes$Length)

write.table(rpkm_values,
            file = out_file,
            sep = "\t",
            quote = FALSE,
            row.names = TRUE, 
            col.names = NA)

cat("RPKM calculation completed! Results saved to:", "\n\t", opt$out, "\n")
