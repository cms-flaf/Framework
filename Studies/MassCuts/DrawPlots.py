import ROOT as rt
import CMS_lumi, tdrstyle
import array

def create_2D_histogram(histogram, canvas, output_file, period):
    """
    Function to create and save a 2D histogram plot with CMS style.

    Args:
    histogram: TH2F, 2D histogram object (MC or data)
    canvas: TCanvas, canvas to draw on
    output_file: str, name of the output file to save the histogram (e.g. 'output.root')

    Returns:
    Saves the canvas in a ROOT file.
    """

    # Set the TDR style
    tdrstyle.setTDRStyle()

    # CMS Lumi settings
    CMS_lumi.extraText = "Preliminary"

    CMS_lumi.relPosX = 0.12

    # Canvas and margin settings
    H_ref = 900
    W_ref = 900
    W = W_ref
    H = H_ref

    T = 0.15 * H_ref
    B = 0.15 * H_ref
    L = 0.25 * W_ref
    R = 0.1 * W_ref

    # Configure canvas
    canvas.SetFillColor(0)
    canvas.SetBorderMode(0)
    canvas.SetFrameFillStyle(0)
    canvas.SetFrameBorderMode(0)
    canvas.SetLeftMargin(L / W)
    canvas.SetRightMargin(R / W)
    canvas.SetTopMargin(T / H)
    canvas.SetBottomMargin(B / H)

    canvas.SetTickx(0)
    canvas.SetTicky(0)
    #canvas.SetTickz(0)

    # Draw the 2D histogram
    histogram.Draw("COLZ")

    # Set axis titles and adjust offsets
    #histogram.GetXaxis().SetTitleOffset(1.2)
    #histogram.GetYaxis().SetTitleOffset(1.4)

    histogram.GetXaxis().SetTitleSize(0.1)
    histogram.GetYaxis().SetTitleSize(0.1)
    # Add CMS and lumi text
    CMS_lumi.CMS_lumi(canvas, period, 0)

    # Update and save the canvas as a PDF file
    canvas.Update()
    canvas.SaveAs(output_file)

    print(f"2D histogram saved to {output_file}")
