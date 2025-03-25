from Bio import SeqIO

def get_main_id(idin):
    if "." in idin:
        return idin.split(".")[0]
    else:
        return idin

def get_fasta_dic(genome_file):
    dic_seq = {}
    with open(genome_file, "r") as f:
        for record in SeqIO.parse(f, "fasta"):
            if str(record.id) != "":
                _seq = str(record.seq)
                dic_seq[get_main_id(str(record.id))] = _seq
    return dic_seq

def get_conver_genome_seq_dic(file:str,
                       conver_dic:dict()):
    """Could give a specific dictionary, to replace the original key
    conver_dic is the conver_dictionary
    """
    dic_seq = {}
    with open(file, "r") as f:
        for record in SeqIO.parse(f, "fasta"):
            print(record.id)
            if str(record.id) != "":
                _seq = str(record.seq)
                if str(record.id) in conver_dic:
                    k = conver_dic[str(record.id)]
                    dic_seq[k] = _seq
    return dic_seq

def get_all_gbff(dic_seq:dict(), gbff_filein:str) -> dict():
    
    def get_main_id(idin):
        if "." in idin:
            return idin.split(".")[0]
        else:
            return idin
    
    dic_gbff = {}
    record_peptides_swith = 0
    with open(gbff_filein, "r") as fh:
        for line in fh:
            line = line.strip()
            if line != "":
                if record_peptides_swith == 1:
                    _translation += line.replace("\"", "")
                    if line[-1] == "\"":
                        if float(len(_translation)) == float(peptides_length):
                            dic_gbff[get_main_id(k)][-2] = True
                        record_peptides_swith = 0
                        _translation = ""
                        
                if line.startswith("VERSION"): ### "ACCESSION"
                    k = get_main_id(line.strip().split(" ")[-1])
                    dic_gbff[get_main_id(k)] = [0, 0, 0, False, False, ""]
                    _seq = dic_seq[get_main_id(k)]
                    # print(_seq)
                    _seq_length = len(_seq)
                    
                elif line.startswith("MANE Ensembl match"):
                    gene_id = line.strip().split(":: ")[1].split("/")[0]
                    dic_gbff[get_main_id(k)][-1] = get_main_id(gene_id)
                
                elif line.startswith("CDS") and (".." in line) and ("join" not in line) and ("<1" not in line) and (">" not in line):
                    """ <1 means incomplete on the 5' end"""
                    """ >999 means incomplete on the 3' end."""
                    tl_num = line.split(" ")[-1].split("..")
                    # print(tl_num)
                    cds_start = int(tl_num[0]) - 1
                    cds_end = int(tl_num[1])
                    cds_length = cds_end - cds_start
                    peptides_length = (cds_length - 3)/float(3)
                    start_codon = _seq[cds_start:cds_start + 3]
                    end_codon = _seq[cds_end - 3:cds_end]
                    if (start_codon == "ATG") and (end_codon in ["TGA", "TAA", "TAG"]):
                        dic_gbff[get_main_id(k)][-3] = True
                        dic_gbff[get_main_id(k)][0] = cds_start  ### 5UTR
                        dic_gbff[get_main_id(k)][1] = cds_length ### CDS
                        dic_gbff[get_main_id(k)][2] = _seq_length - cds_start - cds_length  ### 3UTR
                        
                elif line.startswith("/translation"):
                    record_peptides_swith = 1
                    _translation = line.split("\"")[-1]  
                    
                elif line.startswith("//"):
                    k = "Empty"
                    record_peptides_swith = 0
    return dic_gbff

def filtered_gbff2bed(dic_gbff:dict()):
    data = ""
    for k in dic_gbff.keys():
        if dic_gbff[k][-2:] == [True, True]:
            data += "\t".join([k, "0", str(sum(dic_gbff[k][:3])), "NCBI_GBFF", "0", "+", \
                    ";".join([str(dic_gbff[k][0]), str(dic_gbff[k][2]), str(dic_gbff[k][1])])]) + "\n"
    return data

def filtered_by_representative(bedin:str,
                               filein:str) -> str:
    
    def get_dic(filein:str):
        dic_ids = {}
        dic_ids_del_suffix = {}
        with open(filein, "r") as fh:
            for line in fh:
                tl = line.rstrip().split("\t")
                transcript_id = tl[1] 
                dic_ids[transcript_id] = ""
                dic_ids_del_suffix[transcript_id.split(".")[0]] = "" 
        return dic_ids, dic_ids_del_suffix
    
    rep_dic, rep_dic_del_suffix = get_dic(filein)
    data = ""
    data_del_suffix = ""
    with open(bedin, "r") as fh:
        for line in fh:
            tl =  line.rstrip().split("\t")
            bed_id = tl[0]
            bed_id_del_suffix = bed_id.split(".")[0]
            if bed_id in rep_dic:
                data += line
            if bed_id_del_suffix in rep_dic_del_suffix:
                data_del_suffix += bed_id_del_suffix + "\t" + "\t".join(line.strip().split("\t")[1:]) + "\n"

    return data, data_del_suffix, rep_dic_del_suffix
